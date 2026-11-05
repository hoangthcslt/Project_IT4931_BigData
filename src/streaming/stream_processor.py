"""
Speed Layer: Spark Structured Streaming Processor for Real-Time NYC Subway Flow & Alerts.

Consumes micro-events from Kafka topic `mta-turnstile-stream`, computes 15-minute Sliding
Windows (sliding every 1 minute), joins with Batch Layer Historical Baseline (Mean, Std),
computes Z-Score overcrowding anomalies, and indexes results & critical alerts to Elasticsearch
with sub-3-second end-to-end latency.
"""

import argparse
import glob
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.ingestion.flow_interpolator import TurnstileFlowInterpolator
from src.serving.es_indexer import ElasticsearchServingClient
from src.streaming.anomaly_detector import CRITICAL_Z_THRESHOLD, ZScoreAnomalyDetector


def run_spark_structured_streaming(
    bootstrap_servers: str,
    topic: str,
    baseline_json: str,
    es_host: str,
    alerts_output_json: str,
    checkpoint_dir: str = "data/processed/spark_checkpoint",
) -> None:
    """Run Spark Structured Streaming job over Kafka stream with 15-min / 1-min sliding windows."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    spark = (
        SparkSession.builder.appName("MTA-SpeedLayer-StructuredStreaming")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    event_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("station_id", StringType(), True),
            StructField("complex_id", StringType(), True),
            StructField("stop_name", StringType(), True),
            StructField("borough", StringType(), True),
            StructField("daytime_routes", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("ca", StringType(), True),
            StructField("unit", StringType(), True),
            StructField("scp", StringType(), True),
            StructField("entries_delta", IntegerType(), True),
            StructField("exits_delta", IntegerType(), True),
            StructField("passenger_flow", IntegerType(), True),
            StructField("day_of_week", IntegerType(), True),
            StructField("hour_bucket", IntegerType(), True),
            StructField("mode", StringType(), True),
        ]
    )

    with open(baseline_json, "r", encoding="utf-8") as f:
        baseline_data = json.load(f)

    baseline_schema = StructType(
        [
            StructField("station_id", StringType(), False),
            StructField("day_of_week", IntegerType(), False),
            StructField("hour_bucket", IntegerType(), False),
            StructField("mean_flow_15m", DoubleType(), False),
            StructField("std_flow_15m", DoubleType(), False),
        ]
    )
    baseline_compact = [
        {
            "station_id": str(r["station_id"]),
            "day_of_week": int(r["day_of_week"]),
            "hour_bucket": int(r["hour_bucket"]),
            "mean_flow_15m": float(r["mean_flow_15m"]),
            "std_flow_15m": float(r["std_flow_15m"]),
        }
        for r in baseline_data
    ]
    baseline_df = spark.createDataFrame(baseline_compact, schema=baseline_schema)

    raw_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_kafka_df.selectExpr("CAST(value AS STRING) as json_payload")
        .select(F.from_json(F.col("json_payload"), event_schema).alias("data"))
        .select("data.*")
        .withColumn("ts", F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))
        .filter(F.col("ts").isNotNull())
    )

    # 15-minute Sliding Window, sliding every 1 minute, with 10-minute watermark
    windowed_df = (
        parsed_df.withWatermark("ts", "10 minutes")
        .groupBy(
            F.window("ts", "15 minutes", "1 minute").alias("win"),
            "station_id",
            "stop_name",
            "borough",
            "daytime_routes",
            "latitude",
            "longitude",
        )
        .agg(
            F.sum("entries_delta").alias("entries_15m"),
            F.sum("exits_delta").alias("exits_15m"),
            F.sum("passenger_flow").alias("flow_rate_15m"),
        )
        .withColumn("day_of_week", F.dayofweek(F.col("win.end")))
        .withColumn("hour_bucket", F.hour(F.col("win.end")))
    )

    joined_df = (
        windowed_df.join(
            F.broadcast(baseline_df),
            on=["station_id", "day_of_week", "hour_bucket"],
            how="left",
        )
        .withColumn("baseline_mean_15m", F.coalesce(F.col("mean_flow_15m"), F.lit(80.0)))
        .withColumn("baseline_std_15m", F.coalesce(F.col("std_flow_15m"), F.lit(25.0)))
        .withColumn(
            "z_score",
            (F.col("flow_rate_15m") - F.col("baseline_mean_15m"))
            / F.greatest(F.col("baseline_std_15m"), F.lit(1.0)),
        )
        .withColumn(
            "alert_status",
            F.when(F.col("z_score") > CRITICAL_Z_THRESHOLD, F.lit("CRITICAL_OVERCROWDING_ALERT"))
            .when(F.col("z_score") > 2.0, F.lit("HIGH_CONGESTION"))
            .when(F.col("z_score") > 1.0, F.lit("MODERATE_CROWDING"))
            .otherwise(F.lit("NORMAL")),
        )
    )

    es_client = ElasticsearchServingClient(es_host=es_host)

    def process_micro_batch(batch_df, batch_id: int) -> None:
        t0 = time.perf_counter()
        rows = batch_df.collect()
        if not rows:
            return
        flow_docs = []
        alert_docs = []
        for r in rows:
            rd = r.asDict()
            win = rd["win"]
            doc = {
                "station_id": rd["station_id"],
                "stop_name": rd["stop_name"],
                "borough": rd["borough"],
                "daytime_routes": rd["daytime_routes"],
                "latitude": float(rd["latitude"]),
                "longitude": float(rd["longitude"]),
                "location": {"lat": float(rd["latitude"]), "lon": float(rd["longitude"])},
                "window_start": win["start"].strftime("%Y-%m-%d %H:%M:%S"),
                "window_end": win["end"].strftime("%Y-%m-%d %H:%M:%S"),
                "entries_15m": int(rd["entries_15m"]),
                "exits_15m": int(rd["exits_15m"]),
                "flow_rate_15m": float(rd["flow_rate_15m"]),
                "baseline_mean_15m": round(float(rd["baseline_mean_15m"]), 2),
                "baseline_std_15m": round(float(rd["baseline_std_15m"]), 2),
                "z_score": round(float(rd["z_score"]), 3),
                "alert_status": rd["alert_status"],
                "is_critical_alert": rd["alert_status"] == "CRITICAL_OVERCROWDING_ALERT",
            }
            flow_docs.append(doc)
            if doc["is_critical_alert"]:
                alert_docs.append(doc)

        es_client.bulk_index("mta-realtime-flow", flow_docs)
        if alert_docs:
            es_client.bulk_index("mta-congestion-alerts", alert_docs)

        latency_ms = (time.perf_counter() - t0) * 1000.0
        print(
            f"[Speed Layer Batch #{batch_id}] Windows processed: {len(flow_docs)} | "
            f"Critical Alerts (Z > 3.0): {len(alert_docs)} | Latency: {latency_ms:.1f} ms"
        )

    query = (
        joined_df.writeStream.outputMode("update")
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="2 seconds")
        .start()
    )
    query.awaitTermination()


def run_local_sliding_window_simulation(
    turnstile_pattern: str,
    locations_csv: str,
    baseline_json: str,
    output_json: str,
    es_host: Optional[str] = None,
    max_events: int = 25000,
) -> Dict[str, object]:
    """
    Execute a local micro-batch 15-minute Sliding Window (1-minute slide) simulation
    using TurnstileFlowInterpolator and ZScoreAnomalyDetector, measuring end-to-end latency
    and outputting real-time flow views & Top 5 congested stations.
    """
    t0 = time.perf_counter()
    detector = ZScoreAnomalyDetector(baseline_json_path=baseline_json)
    interpolator = TurnstileFlowInterpolator(
        locations_csv=locations_csv,
        step_minutes=5,
        inject_rush_anomalies=True,
    )

    files = sorted(glob.glob(turnstile_pattern))[:1]
    # Bucket events by (station_id, minute_epoch)
    minute_buckets: Dict[Tuple[str, int], Tuple[int, int]] = defaultdict(lambda: (0, 0))
    station_meta: Dict[str, Dict[str, object]] = {}
    observed_minutes_per_station: Dict[str, List[int]] = defaultdict(list)

    event_count = 0
    for ev in interpolator.stream_from_files(files, mode="interpolate"):
        st_id = str(ev["station_id"])
        station_meta[st_id] = ev
        ts = datetime.strptime(str(ev["event_timestamp"]), "%Y-%m-%d %H:%M:%S")
        min_epoch = int(ts.timestamp() // 60)
        prev_e, prev_x = minute_buckets[(st_id, min_epoch)]
        minute_buckets[(st_id, min_epoch)] = (
            prev_e + int(ev["entries_delta"]),
            prev_x + int(ev["exits_delta"]),
        )
        observed_minutes_per_station[st_id].append(min_epoch)
        event_count += 1
        if event_count >= max_events:
            break

    # Evaluate 15-minute sliding windows across active stations
    evaluated_windows: List[Dict[str, object]] = []
    critical_alerts: List[Dict[str, object]] = []

    for st_id, min_list in observed_minutes_per_station.items():
        meta = station_meta[st_id]
        unique_mins = sorted(set(min_list))
        for win_end_min in unique_mins:
            win_start_min = win_end_min - 15
            e_sum = 0
            x_sum = 0
            for m in range(win_start_min + 1, win_end_min + 1):
                if (st_id, m) in minute_buckets:
                    de, dx = minute_buckets[(st_id, m)]
                    e_sum += de
                    x_sum += dx
            if e_sum + x_sum == 0:
                continue

            end_dt = datetime.fromtimestamp(win_end_min * 60)
            start_dt = datetime.fromtimestamp(win_start_min * 60)
            dow = (end_dt.isoweekday() % 7) + 1
            res = detector.evaluate_window(
                station_id=st_id,
                stop_name=str(meta["stop_name"]),
                borough=str(meta["borough"]),
                daytime_routes=str(meta["daytime_routes"]),
                latitude=float(meta["latitude"]),
                longitude=float(meta["longitude"]),
                window_start=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                window_end=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                entries_15m=e_sum,
                exits_15m=x_sum,
                day_of_week=dow,
                hour_bucket=end_dt.hour,
            )
            evaluated_windows.append(res)
            if res["is_critical_alert"]:
                critical_alerts.append(res)

    # Sort critical alerts by highest Z-Score to extract Top 5 Emergency Stations
    critical_alerts.sort(key=lambda x: float(x["z_score"]), reverse=True)
    top_stations_seen = set()
    top_5_emergency = []
    for alert in critical_alerts:
        if alert["station_id"] not in top_stations_seen:
            top_stations_seen.add(alert["station_id"])
            top_5_emergency.append(alert)
        if len(top_5_emergency) >= 5:
            break

    elapsed_sec = time.perf_counter() - t0
    payload = {
        "streaming_metrics": {
            "micro_events_processed": event_count,
            "sliding_windows_evaluated": len(evaluated_windows),
            "critical_overcrowding_alerts_count": len(critical_alerts),
            "end_to_end_latency_seconds": round(elapsed_sec, 3),
            "throughput_events_per_sec": round(event_count / max(elapsed_sec, 0.001), 1),
            "z_score_threshold": CRITICAL_Z_THRESHOLD,
        },
        "top_5_emergency_stations": top_5_emergency,
        "sample_critical_alerts": critical_alerts[:50],
        "sample_realtime_views": evaluated_windows[:100],
    }

    os.makedirs(os.path.dirname(os.path.abspath(output_json)), exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    if es_host:
        es_client = ElasticsearchServingClient(es_host=es_host)
        es_client.bulk_index("mta-realtime-flow", evaluated_windows[:500])
        if critical_alerts:
            es_client.bulk_index("mta-congestion-alerts", critical_alerts[:200])

    print(f"[Speed Layer Summary] {json.dumps(payload['streaming_metrics'], indent=2)}")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MTA Speed Layer: Spark Structured Streaming & Z-Score Anomaly Processor"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        default="mta-turnstile-stream",
        help="Kafka input topic",
    )
    parser.add_argument(
        "--baseline-json",
        default="data/processed/station_baseline.json",
        help="Historical station baseline JSON path",
    )
    parser.add_argument(
        "--es-host",
        default="http://localhost:9200",
        help="Elasticsearch endpoint URL",
    )
    parser.add_argument(
        "--output",
        default="data/processed/realtime_alerts.json",
        help="Output JSON path for real-time views & congestion alerts",
    )
    parser.add_argument(
        "--engine",
        choices=["auto", "spark", "local"],
        default="auto",
        help="Execution mode: spark (Kafka Structured Streaming), local (simulation), or auto",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=25000,
        help="Number of micro-events to process in local simulation mode",
    )
    args = parser.parse_args()

    if args.engine == "spark":
        run_spark_structured_streaming(
            args.bootstrap_servers, args.topic, args.baseline_json, args.es_host, args.output
        )
    elif args.engine == "local":
        run_local_sliding_window_simulation(
            "data/archive/TurnstileData/turnstile_*.txt",
            "data/archive/MTA_StationLocations.csv",
            args.baseline_json,
            args.output,
            es_host=None,
            max_events=args.max_events,
        )
    else:
        try:
            run_spark_structured_streaming(
                args.bootstrap_servers, args.topic, args.baseline_json, args.es_host, args.output
            )
        except Exception as exc:
            print(
                f"[Speed Layer] Spark/Kafka stream unavailable ({exc}); "
                "running local Sliding Window simulation."
            )
            run_local_sliding_window_simulation(
                "data/archive/TurnstileData/turnstile_*.txt",
                "data/archive/MTA_StationLocations.csv",
                args.baseline_json,
                args.output,
                es_host=None,
                max_events=args.max_events,
            )


if __name__ == "__main__":
    main()
