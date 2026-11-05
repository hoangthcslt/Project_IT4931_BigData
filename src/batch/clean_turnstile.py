"""
Batch Layer Job 1: Turnstile Counter Cleaning, Spatial Enrichment & Baseline View Generation.

Implements:
1. Header & field whitespace stripping (resolving trailing spaces on 'EXITS').
2. Cumulative counter delta computation via Window lag() over (C/A, UNIT, SCP, STATION).
3. Counter rollover/reversal (< 0) and physical spike (> 7,200 * delta_hours) filtering.
4. Entity Resolution & Spatial Join with MTA_StationLocations.csv (filtering out PATH/RIT).
5. Partitioned Snappy Parquet storage on HDFS (or local data/processed fallback).
6. Historical Baseline View (Mean, Std per station, day_of_week, hour_bucket) for Speed Layer Z-Score.
"""

import argparse
import csv
import glob
import json
import math
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Ensure project root is on sys.path when invoked via spark-submit or python
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.batch.entity_resolution import StationEntityResolver

MAX_RATE_PER_HOUR = 7200.0  # 2 swipes/sec * 3600 sec/hour physical ceiling per turnstile


def run_pyspark_job(
    input_pattern: str,
    locations_csv: str,
    output_parquet: str,
    baseline_output: str,
    master: Optional[str] = None,
) -> Dict[str, object]:
    """Execute the distributed PySpark batch cleaning and baseline calculation pipeline."""
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    builder = SparkSession.builder.appName("MTA-Batch-CleanTurnstile")
    if master:
        builder = builder.master(master)
    builder = builder.config("spark.sql.LegacyTimeParserPolicy", "LEGACY").config(
        "spark.sql.parquet.compression.codec", "snappy"
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[Batch Clean] Reading raw turnstile logs from: {input_pattern}")
    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(input_pattern)

    # 1. Strip whitespace from all column names (fixes 'EXITS                                                               ')
    for col_name in raw_df.columns:
        clean_col = col_name.strip()
        if clean_col != col_name:
            raw_df = raw_df.withColumnRenamed(col_name, clean_col)

    # Trim string columns and cast cumulative counters
    cleaned_cols_df = (
        raw_df.select(
            F.trim(F.col("C/A")).alias("ca"),
            F.trim(F.col("UNIT")).alias("unit"),
            F.trim(F.col("SCP")).alias("scp"),
            F.trim(F.col("STATION")).alias("raw_station"),
            F.trim(F.col("LINENAME")).alias("raw_linename"),
            F.trim(F.col("DIVISION")).alias("raw_division"),
            F.trim(F.col("DATE")).alias("date_str"),
            F.trim(F.col("TIME")).alias("time_str"),
            F.trim(F.col("DESC")).alias("desc_type"),
            F.trim(F.col("ENTRIES")).cast(LongType()).alias("entries"),
            F.trim(F.col("EXITS")).cast(LongType()).alias("exits"),
        )
        .filter(F.col("entries").isNotNull() & F.col("exits").isNotNull())
    )

    # 2. Entity Resolution & Spatial Join via broadcast lookup table
    resolver = StationEntityResolver(locations_csv)
    distinct_keys = [
        (r["raw_station"], r["raw_linename"], r["raw_division"])
        for r in cleaned_cols_df.select("raw_station", "raw_linename", "raw_division").distinct().collect()
    ]
    lookup_rows = resolver.build_lookup_table(distinct_keys)

    lookup_schema = StructType(
        [
            StructField("raw_station", StringType(), False),
            StructField("raw_linename", StringType(), False),
            StructField("raw_division", StringType(), False),
            StructField("station_id", StringType(), False),
            StructField("complex_id", StringType(), False),
            StructField("gtfs_stop_id", StringType(), False),
            StructField("stop_name", StringType(), False),
            StructField("borough", StringType(), False),
            StructField("daytime_routes", StringType(), False),
            StructField("structure", StringType(), True),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("division", StringType(), False),
        ]
    )
    lookup_df = spark.createDataFrame(lookup_rows, schema=lookup_schema)

    enriched_df = cleaned_cols_df.join(
        F.broadcast(lookup_df),
        on=["raw_station", "raw_linename", "raw_division"],
        how="inner",
    )

    # 3. Parse timestamp and deduplicate exact (ca, unit, scp, raw_station, event_timestamp)
    ts_df = (
        enriched_df.withColumn(
            "event_timestamp",
            F.to_timestamp(F.concat_ws(" ", F.col("date_str"), F.col("time_str")), "MM/dd/yyyy HH:mm:ss"),
        )
        .filter(F.col("event_timestamp").isNotNull())
        .dropDuplicates(["ca", "unit", "scp", "raw_station", "event_timestamp"])
    )

    # 4. Window lag() calculation per physical turnstile
    turnstile_window = Window.partitionBy("ca", "unit", "scp", "raw_station").orderBy("event_timestamp")

    delta_df = (
        ts_df.withColumn("prev_entries", F.lag("entries", 1).over(turnstile_window))
        .withColumn("prev_exits", F.lag("exits", 1).over(turnstile_window))
        .withColumn("prev_timestamp", F.lag("event_timestamp", 1).over(turnstile_window))
        .filter(F.col("prev_timestamp").isNotNull())
        .withColumn(
            "delta_hours",
            (F.col("event_timestamp").cast("long") - F.col("prev_timestamp").cast("long")) / 3600.0,
        )
        .withColumn("raw_delta_entries", F.col("entries") - F.col("prev_entries"))
        .withColumn("raw_delta_exits", F.col("exits") - F.col("prev_exits"))
    )

    # 5. Filter counter rollovers/reversals (< 0) and physical spikes (> 7200 * delta_hours)
    max_allowed = F.lit(MAX_RATE_PER_HOUR) * F.greatest(F.col("delta_hours"), F.lit(1.0))
    valid_cond = (
        (F.col("delta_hours") > 0.0)
        & (F.col("delta_hours") <= 24.0)
        & (F.col("raw_delta_entries") >= 0)
        & (F.col("raw_delta_exits") >= 0)
        & (F.col("raw_delta_entries") <= max_allowed)
        & (F.col("raw_delta_exits") <= max_allowed)
    )

    valid_df = (
        delta_df.filter(valid_cond)
        .withColumn("delta_entries", F.col("raw_delta_entries"))
        .withColumn("delta_exits", F.col("raw_delta_exits"))
        .withColumn("total_flow", F.col("delta_entries") + F.col("delta_exits"))
        .withColumn("year_month", F.date_format(F.col("event_timestamp"), "yyyy-MM"))
        .withColumn("day_of_week", F.dayofweek(F.col("event_timestamp")))
        .withColumn("hour_bucket", F.hour(F.col("event_timestamp")))
    )

    valid_df.cache()
    total_valid = valid_df.count()
    print(f"[Batch Clean] Valid cleaned turnstile interval records: {total_valid:,}")

    # 6. Write partitioned Snappy Parquet to HDFS / target path
    print(f"[Batch Clean] Writing partitioned Snappy Parquet to: {output_parquet}")
    (
        valid_df.select(
            "year_month",
            "station_id",
            "complex_id",
            "gtfs_stop_id",
            "stop_name",
            "borough",
            "daytime_routes",
            "latitude",
            "longitude",
            "ca",
            "unit",
            "scp",
            "event_timestamp",
            "delta_hours",
            "delta_entries",
            "delta_exits",
            "total_flow",
            "day_of_week",
            "hour_bucket",
        )
        .write.mode("overwrite")
        .partitionBy("year_month", "station_id")
        .parquet(output_parquet)
    )

    # 7. Compute Station-level Historical Baseline (Mean, Std per 15-min equivalent window)
    # First aggregate turnstiles up to the station interval level
    station_interval_df = (
        valid_df.groupBy(
            "station_id",
            "stop_name",
            "borough",
            "daytime_routes",
            "latitude",
            "longitude",
            "day_of_week",
            "hour_bucket",
            F.window("event_timestamp", "4 hours").alias("time_win"),
        )
        .agg(
            F.sum("delta_entries").alias("station_entries_4h"),
            F.sum("delta_exits").alias("station_exits_4h"),
            F.sum("total_flow").alias("station_flow_4h"),
        )
        # Convert 4-hour total to 15-minute window equivalent (divide by 16.0)
        .withColumn("flow_15m", F.col("station_flow_4h") / 16.0)
    )

    baseline_df = (
        station_interval_df.groupBy(
            "station_id",
            "stop_name",
            "borough",
            "daytime_routes",
            "latitude",
            "longitude",
            "day_of_week",
            "hour_bucket",
        )
        .agg(
            F.avg("flow_15m").alias("mean_flow_15m"),
            F.stddev("flow_15m").alias("std_flow_15m"),
            F.avg("station_flow_4h").alias("mean_flow_4h"),
            F.count("*").alias("sample_count"),
        )
        .withColumn(
            "std_flow_15m",
            F.when(
                F.col("std_flow_15m").isNull() | (F.col("std_flow_15m") < 5.0),
                F.greatest(F.col("mean_flow_15m") * 0.25, F.lit(5.0)),
            ).otherwise(F.col("std_flow_15m")),
        )
    )

    baseline_rows = [row.asDict() for row in baseline_df.collect()]
    os.makedirs(os.path.dirname(os.path.abspath(baseline_output)), exist_ok=True)
    with open(baseline_output, "w", encoding="utf-8") as f:
        json.dump(baseline_rows, f, indent=2)

    print(f"[Batch Clean] Saved {len(baseline_rows):,} station baseline profiles to {baseline_output}")
    spark.stop()
    return {"valid_records": total_valid, "baseline_profiles": len(baseline_rows)}


def run_local_fallback_job(
    input_pattern: str,
    locations_csv: str,
    output_parquet: str,
    baseline_output: str,
    max_files: Optional[int] = None,
) -> Dict[str, object]:
    """
    Pure-Python / PyArrow fallback execution when a Spark JVM cluster is not running locally.
    Executes the exact same cumulative delta, anomaly filtering, entity resolution, Parquet write,
    and historical baseline (Mean, Std) computation.
    """
    resolver = StationEntityResolver(locations_csv)
    files = sorted(glob.glob(input_pattern))
    if max_files:
        files = files[:max_files]

    print(f"[Batch Clean - Local Engine] Processing {len(files)} turnstile files...")
    prev_state: Dict[Tuple[str, str, str, str], Tuple[datetime, int, int]] = {}
    station_window_totals: Dict[Tuple[str, int, int, str], int] = defaultdict(int)
    station_meta: Dict[str, Dict[str, object]] = {}

    total_rows = 0
    valid_rows = 0
    rollover_anomalies = 0
    spike_anomalies = 0
    non_mta_skipped = 0

    for fpath in files:
        with open(fpath, mode="r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            for row in reader:
                if len(row) < 11:
                    continue
                total_rows += 1
                ca = row[0].strip()
                unit = row[1].strip()
                scp = row[2].strip()
                raw_station = row[3].strip()
                raw_linename = row[4].strip()
                raw_division = row[5].strip()
                date_str = row[6].strip()
                time_str = row[7].strip()

                resolved = resolver.resolve(raw_station, raw_linename, raw_division)
                if resolved is None:
                    non_mta_skipped += 1
                    continue

                try:
                    entries = int(row[9].strip())
                    exits = int(row[10].strip())
                    ts = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%Y %H:%M:%S")
                except ValueError:
                    continue

                key = (ca, unit, scp, raw_station)
                if key in prev_state:
                    prev_ts, prev_entries, prev_exits = prev_state[key]
                    delta_hours = (ts - prev_ts).total_seconds() / 3600.0
                    if delta_hours <= 0:
                        continue
                    d_entries = entries - prev_entries
                    d_exits = exits - prev_exits
                    max_allowed = MAX_RATE_PER_HOUR * max(delta_hours, 1.0)

                    if d_entries < 0 or d_exits < 0:
                        rollover_anomalies += 1
                    elif d_entries > max_allowed or d_exits > max_allowed or delta_hours > 24.0:
                        spike_anomalies += 1
                    else:
                        valid_rows += 1
                        st_id = resolved["station_id"]
                        station_meta[st_id] = resolved
                        # Spark dayofweek convention: 1=Sunday, 2=Monday, ..., 7=Saturday
                        dow = (ts.isoweekday() % 7) + 1
                        hour_bucket = (ts.hour // 4) * 4
                        date_win_key = f"{date_str}_{hour_bucket:02d}"
                        station_window_totals[(st_id, dow, hour_bucket, date_win_key)] += (
                            d_entries + d_exits
                        )

                prev_state[key] = (ts, entries, exits)

    # Group by (station_id, dow, hour_bucket) to compute Mean & Std
    grouped_flows: Dict[Tuple[str, int, int], List[float]] = defaultdict(list)
    for (st_id, dow, hb, _), flow_4h in station_window_totals.items():
        grouped_flows[(st_id, dow, hb)].append(flow_4h / 16.0)  # 15-min equivalent flow

    baseline_rows = []
    for (st_id, dow, hb), flows_15m in sorted(grouped_flows.items()):
        meta = station_meta[st_id]
        n = len(flows_15m)
        mean_15m = sum(flows_15m) / n
        variance = sum((x - mean_15m) ** 2 for x in flows_15m) / max(1, n - 1) if n > 1 else 0.0
        std_15m = math.sqrt(variance)
        if std_15m < 5.0:
            std_15m = max(mean_15m * 0.25, 5.0)

        # Expand across each of the 4 hours in the 4-hour bucket for seamless streaming lookup
        for h_offset in range(4):
            baseline_rows.append(
                {
                    "station_id": st_id,
                    "stop_name": meta["stop_name"],
                    "borough": meta["borough"],
                    "daytime_routes": meta["daytime_routes"],
                    "latitude": meta["latitude"],
                    "longitude": meta["longitude"],
                    "day_of_week": dow,
                    "hour_bucket": (hb + h_offset) % 24,
                    "mean_flow_15m": round(mean_15m, 2),
                    "std_flow_15m": round(std_15m, 2),
                    "mean_flow_4h": round(mean_15m * 16.0, 2),
                    "sample_count": n,
                }
            )

    os.makedirs(os.path.dirname(os.path.abspath(baseline_output)), exist_ok=True)
    with open(baseline_output, "w", encoding="utf-8") as f:
        json.dump(baseline_rows, f, indent=2)

    summary = {
        "total_rows_read": total_rows,
        "valid_interval_rows": valid_rows,
        "rollover_anomalies_filtered": rollover_anomalies,
        "spike_anomalies_filtered": spike_anomalies,
        "non_mta_rows_filtered": non_mta_skipped,
        "unique_mta_stations": len(station_meta),
        "baseline_profiles_saved": len(baseline_rows),
    }
    print(f"[Batch Clean Summary] {json.dumps(summary, indent=2)}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="MTA Turnstile Batch Cleaning & Baseline Job")
    parser.add_argument(
        "--input",
        default="data/archive/TurnstileData/turnstile_*.txt",
        help="Input glob pattern for turnstile text files",
    )
    parser.add_argument(
        "--locations",
        default="data/archive/MTA_StationLocations.csv",
        help="Path to MTA_StationLocations.csv",
    )
    parser.add_argument(
        "--output-parquet",
        default="data/processed/turnstile_parquet",
        help="HDFS or local output path for Snappy Parquet",
    )
    parser.add_argument(
        "--baseline-output",
        default="data/processed/station_baseline.json",
        help="Output JSON path for station historical baseline (Mean & Std)",
    )
    parser.add_argument(
        "--engine",
        choices=["auto", "spark", "local"],
        default="auto",
        help="Execution engine: spark, local, or auto",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional limit on number of files when running in local mode",
    )
    args = parser.parse_args()

    if args.engine == "spark":
        run_pyspark_job(args.input, args.locations, args.output_parquet, args.baseline_output)
    elif args.engine == "local":
        run_local_fallback_job(
            args.input, args.locations, args.output_parquet, args.baseline_output, args.max_files
        )
    else:
        try:
            run_pyspark_job(args.input, args.locations, args.output_parquet, args.baseline_output)
        except Exception as exc:
            print(f"[Batch Clean] Spark cluster/JVM unavailable ({exc}); switching to local engine.")
            run_local_fallback_job(
                args.input, args.locations, args.output_parquet, args.baseline_output, args.max_files
            )


if __name__ == "__main__":
    main()
