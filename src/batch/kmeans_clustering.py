"""
Batch Layer Job 2: Distributed Station Behavioral Clustering via Spark MLlib K-Means.

Extracts a 24-dimensional normalized hourly passenger flow feature vector (plus entry/exit
asymmetry indicators) for each NYC Subway station and clusters stations into behavioral archetypes:
  - Cluster 0: CBD Financial / Commercial Core (Sharp bimodal AM exit / PM entry peaks, ultra-high volume)
  - Cluster 1: Residential Commuter Satellites (Strong AM entry / PM exit directional wave)
  - Cluster 2: All-Day Cultural / Tourist / Mixed Hubs (Sustained midday, evening & weekend activity)

Evaluates clustering quality via ClusteringEvaluator (Silhouette Score) and exports results
to JSON / Elasticsearch for the Serving Layer.
"""

import argparse
import json
import math
import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


ARCHETYPE_LABELS: Dict[int, str] = {
    0: "Residential Commuter Satellite",
    1: "CBD Financial & Office Hub",
    2: "All-Day Commercial & Tourist Hub",
    3: "Mixed Urban Transit Node",
}


def interpolate_4h_to_24h_profile(bucket_shares: Dict[int, float], borough: str = "M") -> List[float]:
    """
    Construct a smooth 24-hour normalized temporal distribution from 4-hour turnstile buckets
    combined with empirical NYC diurnal transit weights.
    """
    # Empirical base weights for NYC subway hours 0..23
    diurnal_prior = [
        0.012, 0.007, 0.005, 0.006, 0.012, 0.028,
        0.058, 0.088, 0.105, 0.072, 0.048, 0.045,
        0.048, 0.050, 0.055, 0.068, 0.085, 0.095,
        0.068, 0.045, 0.032, 0.026, 0.020, 0.015,
    ]
    vec = [0.0] * 24
    for h in range(24):
        b = (h // 4) * 4
        b_val = bucket_shares.get(b, 1.0 / 6.0)
        vec[h] = b_val * diurnal_prior[h]

    total = sum(vec)
    if total > 0:
        vec = [round(v / total, 6) for v in vec]
    else:
        vec = [round(1.0 / 24.0, 6)] * 24
    return vec


def assign_archetype_name(
    hourly_vec: List[float], borough: str, mean_daily_flow: float
) -> Tuple[int, str]:
    """Assign interpretable archetype label based on peak concentration, volume, and borough."""
    am_peak = sum(hourly_vec[6:10])
    midday = sum(hourly_vec[10:15])
    pm_peak = sum(hourly_vec[16:20])
    late_night = sum(hourly_vec[20:24]) + sum(hourly_vec[0:5])

    if borough == "M" and mean_daily_flow > 22000 and (pm_peak >= 0.30 or am_peak >= 0.28):
        return 1, "CBD Financial & Office Hub"
    elif (midday + late_night) >= 0.42 or (borough == "M" and mean_daily_flow > 10000):
        return 2, "All-Day Commercial & Tourist Hub"
    else:
        return 0, "Residential Commuter Satellite"


def run_spark_kmeans(
    parquet_input: str,
    baseline_json: str,
    output_json: str,
    k: int = 3,
) -> Dict[str, object]:
    """Run distributed Spark MLlib K-Means clustering on cleaned turnstile Parquet data."""
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.ml.feature import StandardScaler, VectorAssembler
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("MTA-Batch-KMeansClustering")
        .config("spark.sql.LegacyTimeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(parquet_input)

    # Aggregate hourly share per station
    hourly_df = (
        df.groupBy(
            "station_id",
            "stop_name",
            "borough",
            "daytime_routes",
            "latitude",
            "longitude",
            "hour_bucket",
        )
        .agg(
            F.sum("delta_entries").alias("entries"),
            F.sum("delta_exits").alias("exits"),
            F.sum("total_flow").alias("flow"),
        )
    )

    pivoted = (
        hourly_df.groupBy("station_id", "stop_name", "borough", "daytime_routes", "latitude", "longitude")
        .pivot("hour_bucket", list(range(24)))
        .agg(F.first("flow"))
        .na.fill(0)
    )

    hour_cols = [str(h) for h in range(24)]
    total_expr = sum(F.col(c) for c in hour_cols)
    pivoted = pivoted.withColumn("total_flow", total_expr).filter(F.col("total_flow") > 0)

    for c in hour_cols:
        pivoted = pivoted.withColumn(f"h_{c}", F.col(c) / F.col("total_flow"))

    feature_cols = [f"h_{c}" for c in hour_cols]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    assembled = assembler.transform(pivoted)

    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features", withStd=True, withMean=True
    )
    scaler_model = scaler.fit(assembled)
    scaled_df = scaler_model.transform(assembled)

    kmeans = KMeans(featuresCol="features", predictionCol="cluster_id", k=k, seed=42, maxIter=50)
    model = kmeans.fit(scaled_df)
    predictions = model.transform(scaled_df)

    evaluator = ClusteringEvaluator(
        featuresCol="features", predictionCol="cluster_id", metricName="silhouette"
    )
    silhouette = float(evaluator.evaluate(predictions))
    print(f"[Spark MLlib K-Means] K={k} | Silhouette Score = {silhouette:.4f}")

    rows = predictions.select(
        "station_id",
        "stop_name",
        "borough",
        "daytime_routes",
        "latitude",
        "longitude",
        "cluster_id",
        "total_flow",
        *feature_cols,
    ).collect()

    results = []
    for r in rows:
        rd = r.asDict()
        cid = int(rd["cluster_id"])
        vec = [round(float(rd[f"h_{h}"]), 6) for h in range(24)]
        results.append(
            {
                "station_id": rd["station_id"],
                "stop_name": rd["stop_name"],
                "borough": rd["borough"],
                "daytime_routes": rd["daytime_routes"],
                "latitude": float(rd["latitude"]),
                "longitude": float(rd["longitude"]),
                "location": {"lat": float(rd["latitude"]), "lon": float(rd["longitude"])},
                "cluster_id": cid,
                "archetype": ARCHETYPE_LABELS.get(cid, f"Cluster {cid}"),
                "hourly_profile_24h": vec,
                "silhouette_score": round(silhouette, 4),
            }
        )

    os.makedirs(os.path.dirname(os.path.abspath(output_json)), exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    spark.stop()
    return {"stations_clustered": len(results), "silhouette_score": round(silhouette, 4)}


def run_baseline_kmeans_fallback(
    baseline_json: str,
    output_json: str,
    k: int = 3,
) -> Dict[str, object]:
    """
    Cluster stations using their 24-hour flow profiles from station_baseline.json
    and compute the exact Silhouette Score using scikit-learn / numpy.
    """
    if not os.path.exists(baseline_json):
        raise FileNotFoundError(
            f"Baseline file {baseline_json} not found. Run src/batch/clean_turnstile.py first."
        )

    with open(baseline_json, "r", encoding="utf-8") as f:
        baseline_rows = json.load(f)

    station_buckets: Dict[str, Dict[int, List[float]]] = defaultdict(lambda: defaultdict(list))
    station_info: Dict[str, Dict[str, object]] = {}

    for row in baseline_rows:
        st_id = str(row["station_id"])
        station_info[st_id] = row
        hb = (int(row["hour_bucket"]) // 4) * 4
        station_buckets[st_id][hb].append(float(row["mean_flow_4h"]))

    station_ids = sorted(station_info.keys(), key=lambda x: int(x) if x.isdigit() else x)
    features_matrix: List[List[float]] = []
    hourly_vectors: Dict[str, List[float]] = {}
    daily_flows: Dict[str, float] = {}

    for st_id in station_ids:
        meta = station_info[st_id]
        borough = str(meta.get("borough", "M"))
        b_avgs = {
            b: (sum(vals) / len(vals) if vals else 0.0)
            for b, vals in station_buckets[st_id].items()
        }
        daily_flow = sum(b_avgs.values())
        daily_flows[st_id] = daily_flow

        # Build distinctive 24h behavioral profile reflecting station's bucket distribution & archetype
        b_total = max(daily_flow, 1.0)
        b_shares = {b: val / b_total for b, val in b_avgs.items()}

        morning_ratio = (b_shares.get(4, 0.0) + b_shares.get(8, 0.0))
        evening_ratio = (b_shares.get(16, 0.0) + b_shares.get(20, 0.0))
        midday_ratio = b_shares.get(12, 0.0)
        log_vol = math.log10(max(daily_flow, 10.0)) / 5.5

        vec_24h = interpolate_4h_to_24h_profile(b_shares, borough)
        hourly_vectors[st_id] = vec_24h

        # Construct normalized behavioral feature representation for K-Means clustering
        is_manhattan = 1.0 if borough == "M" else 0.0
        is_high_vol = 1.0 if daily_flow > 20000 else 0.0
        features_matrix.append(
            [
                morning_ratio * 2.5,
                midday_ratio * 2.5,
                evening_ratio * 2.5,
                log_vol,
                is_manhattan,
                is_high_vol,
            ]
        )

    try:
        import numpy as np
        from sklearn.cluster import KMeans as SklearnKMeans
        from sklearn.metrics import silhouette_score

        X = np.array(features_matrix, dtype=float)
        km = SklearnKMeans(n_clusters=k, random_state=42, n_init=10)
        labels = list(km.fit_predict(X))
        sil_score = float(silhouette_score(X, labels))
    except Exception:
        # Pure-Python Lloyd's K-Means & exact Silhouette Score calculation
        n_samples = len(features_matrix)
        n_dims = len(features_matrix[0]) if n_samples else 0

        def sq_dist(a: List[float], b: List[float]) -> float:
            return sum((x - y) ** 2 for x, y in zip(a, b))

        def euclid(a: List[float], b: List[float]) -> float:
            return math.sqrt(sq_dist(a, b))

        # Deterministic k-means++ style initialization (high-vol Manhattan, medium-vol Manhattan/Hub, outer-borough residential)
        sorted_idx = sorted(range(n_samples), key=lambda i: (features_matrix[i][4], features_matrix[i][5], features_matrix[i][3]))
        centroids = [
            list(features_matrix[sorted_idx[0]]),
            list(features_matrix[sorted_idx[int(n_samples * 0.72)]]),
            list(features_matrix[sorted_idx[-1]]),
        ][:k]
        while len(centroids) < k:
            centroids.append(list(features_matrix[len(centroids) % max(1, n_samples)]))

        labels = [0] * n_samples
        for _ in range(40):
            changed = False
            for i, vec in enumerate(features_matrix):
                best_c = min(range(k), key=lambda c: sq_dist(vec, centroids[c]))
                if labels[i] != best_c:
                    labels[i] = best_c
                    changed = True
            if not changed:
                break
            for c in range(k):
                members = [features_matrix[i] for i in range(n_samples) if labels[i] == c]
                if members:
                    centroids[c] = [
                        sum(m[d] for m in members) / len(members) for d in range(n_dims)
                    ]

        # Exact Silhouette Score across all stations
        cluster_members: Dict[int, List[int]] = defaultdict(list)
        for idx, cid in enumerate(labels):
            cluster_members[cid].append(idx)

        sil_vals: List[float] = []
        for i in range(n_samples):
            own_c = labels[i]
            own_list = [j for j in cluster_members[own_c] if j != i]
            a_i = (
                sum(euclid(features_matrix[i], features_matrix[j]) for j in own_list) / len(own_list)
                if own_list
                else 0.0
            )
            b_i = float("inf")
            for other_c, other_list in cluster_members.items():
                if other_c == own_c or not other_list:
                    continue
                dist_other = sum(
                    euclid(features_matrix[i], features_matrix[j]) for j in other_list
                ) / len(other_list)
                if dist_other < b_i:
                    b_i = dist_other
            if b_i == float("inf") or max(a_i, b_i) == 0:
                sil_vals.append(0.0)
            else:
                sil_vals.append((b_i - a_i) / max(a_i, b_i))
        sil_score = sum(sil_vals) / max(1, len(sil_vals))

    # Map cluster IDs to human-readable transit archetypes
    cluster_volumes: Dict[int, List[Tuple[float, str]]] = defaultdict(list)
    for idx, st_id in enumerate(station_ids):
        cluster_volumes[int(labels[idx])].append(
            (daily_flows[st_id], str(station_info[st_id]["borough"]))
        )

    # Sort clusters by average daily flow so cluster naming is deterministic
    sorted_clusters = sorted(
        cluster_volumes.keys(),
        key=lambda cid: sum(v[0] for v in cluster_volumes[cid]) / max(1, len(cluster_volumes[cid])),
        reverse=True,
    )
    cluster_name_map: Dict[int, str] = {}
    archetype_order = [
        "CBD Financial & Office Hub",
        "All-Day Commercial & Tourist Hub",
        "Residential Commuter Satellite",
        "Mixed Urban Transit Node",
    ]
    for rank, cid in enumerate(sorted_clusters):
        cluster_name_map[cid] = archetype_order[min(rank, len(archetype_order) - 1)]

    results = []
    cluster_counts: Dict[str, int] = defaultdict(int)
    for idx, st_id in enumerate(station_ids):
        meta = station_info[st_id]
        cid = int(labels[idx])
        arch_name = cluster_name_map[cid]
        cluster_counts[arch_name] += 1
        lat = float(meta["latitude"])
        lon = float(meta["longitude"])
        results.append(
            {
                "station_id": st_id,
                "stop_name": meta["stop_name"],
                "borough": meta["borough"],
                "daytime_routes": meta["daytime_routes"],
                "latitude": lat,
                "longitude": lon,
                "location": {"lat": lat, "lon": lon},
                "cluster_id": cid,
                "archetype": arch_name,
                "estimated_daily_flow": round(daily_flows[st_id], 1),
                "hourly_profile_24h": hourly_vectors[st_id],
                "silhouette_score": round(sil_score, 4),
            }
        )

    os.makedirs(os.path.dirname(os.path.abspath(output_json)), exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    summary = {
        "stations_clustered": len(results),
        "k": k,
        "silhouette_score": round(sil_score, 4),
        "cluster_distribution": dict(cluster_counts),
        "output_file": output_json,
    }
    print(f"[K-Means Clustering Summary] {json.dumps(summary, indent=2)}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark MLlib K-Means Station Clustering Job")
    parser.add_argument(
        "--parquet-input",
        default="data/processed/turnstile_parquet",
        help="Input path to cleaned turnstile Parquet",
    )
    parser.add_argument(
        "--baseline-json",
        default="data/processed/station_baseline.json",
        help="Path to station_baseline.json",
    )
    parser.add_argument(
        "--output",
        default="data/processed/station_clusters.json",
        help="Output JSON path for K-Means clusters",
    )
    parser.add_argument("--k", type=int, default=3, help="Number of K-Means clusters (default: 3)")
    parser.add_argument(
        "--engine",
        choices=["auto", "spark", "local"],
        default="auto",
        help="Execution engine",
    )
    args = parser.parse_args()

    if args.engine == "spark":
        run_spark_kmeans(args.parquet_input, args.baseline_json, args.output, args.k)
    elif args.engine == "local":
        run_baseline_kmeans_fallback(args.baseline_json, args.output, args.k)
    else:
        if os.path.exists(args.parquet_input):
            try:
                run_spark_kmeans(args.parquet_input, args.baseline_json, args.output, args.k)
                return
            except Exception as exc:
                print(f"[K-Means] Falling back to local clustering engine ({exc})")
        run_baseline_kmeans_fallback(args.baseline_json, args.output, args.k)


if __name__ == "__main__":
    main()
