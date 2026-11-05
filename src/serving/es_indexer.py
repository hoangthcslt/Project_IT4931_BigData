"""
Serving Layer: Elasticsearch Geo-Point Indexer & Batch/Speed View Synchronizer.

Manages Elasticsearch indices for the Lambda Architecture Serving Layer:
  1. `mta-station-clusters`: Spark MLlib K-Means station behavioral archetypes + `geo_point`
  2. `mta-turnstile-baseline`: Batch Layer historical Mean & Std per station/day/hour + `geo_point`
  3. `mta-realtime-flow`: Speed Layer 15-min sliding window passenger flow & Z-Score + `geo_point`
  4. `mta-congestion-alerts`: Real-time `CRITICAL_OVERCROWDING_ALERT` stream (Z-Score > 3.0)
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Dict, List, Optional

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

INDEX_MAPPINGS: Dict[str, Dict[str, object]] = {
    "mta-station-clusters": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "station_id": {"type": "keyword"},
                "stop_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "borough": {"type": "keyword"},
                "daytime_routes": {"type": "keyword"},
                "latitude": {"type": "double"},
                "longitude": {"type": "double"},
                "location": {"type": "geo_point"},
                "cluster_id": {"type": "integer"},
                "archetype": {"type": "keyword"},
                "estimated_daily_flow": {"type": "double"},
                "silhouette_score": {"type": "double"},
            }
        },
    },
    "mta-turnstile-baseline": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "station_id": {"type": "keyword"},
                "stop_name": {"type": "keyword"},
                "borough": {"type": "keyword"},
                "daytime_routes": {"type": "keyword"},
                "location": {"type": "geo_point"},
                "day_of_week": {"type": "integer"},
                "hour_bucket": {"type": "integer"},
                "mean_flow_15m": {"type": "double"},
                "std_flow_15m": {"type": "double"},
                "mean_flow_4h": {"type": "double"},
                "sample_count": {"type": "integer"},
            }
        },
    },
    "mta-realtime-flow": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "station_id": {"type": "keyword"},
                "stop_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "borough": {"type": "keyword"},
                "daytime_routes": {"type": "keyword"},
                "location": {"type": "geo_point"},
                "window_start": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time"},
                "window_end": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time"},
                "entries_15m": {"type": "integer"},
                "exits_15m": {"type": "integer"},
                "flow_rate_15m": {"type": "double"},
                "baseline_mean_15m": {"type": "double"},
                "baseline_std_15m": {"type": "double"},
                "z_score": {"type": "double"},
                "alert_status": {"type": "keyword"},
                "heatmap_color": {"type": "keyword"},
                "is_critical_alert": {"type": "boolean"},
            }
        },
    },
    "mta-congestion-alerts": {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "station_id": {"type": "keyword"},
                "stop_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                "borough": {"type": "keyword"},
                "daytime_routes": {"type": "keyword"},
                "location": {"type": "geo_point"},
                "window_start": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time"},
                "window_end": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time"},
                "flow_rate_15m": {"type": "double"},
                "baseline_mean_15m": {"type": "double"},
                "baseline_std_15m": {"type": "double"},
                "z_score": {"type": "double"},
                "alert_status": {"type": "keyword"},
                "heatmap_color": {"type": "keyword"},
                "is_critical_alert": {"type": "boolean"},
            }
        },
    },
}


class ElasticsearchServingClient:
    """Lightweight REST client for Elasticsearch 7.x Serving Layer with offline fallback."""

    def __init__(self, es_host: str = "http://localhost:9200"):
        self.es_host = es_host.rstrip("/")

    def is_reachable(self) -> bool:
        try:
            req = urllib.request.Request(self.es_host, method="GET")
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                return resp.status == 200
        except Exception:
            return False

    def init_indices(self, save_local_schema: str = "data/processed/es_index_mappings.json") -> Dict[str, str]:
        """Create Elasticsearch indices with geo_point mappings (and persist schema snapshot)."""
        os.makedirs(os.path.dirname(os.path.abspath(save_local_schema)), exist_ok=True)
        with open(save_local_schema, "w", encoding="utf-8") as f:
            json.dump(INDEX_MAPPINGS, f, indent=2)

        status_report: Dict[str, str] = {}
        reachable = self.is_reachable()

        for index_name, mapping_body in INDEX_MAPPINGS.items():
            if not reachable:
                status_report[index_name] = "schema_saved_locally (ES container offline)"
                continue
            url = f"{self.es_host}/{index_name}"
            data = json.dumps(mapping_body).encode("utf-8")
            req = urllib.request.Request(
                url, data=data, headers={"Content-Type": "application/json"}, method="PUT"
            )
            try:
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    status_report[index_name] = f"created (HTTP {resp.status})"
            except urllib.error.HTTPError as e:
                if e.code == 400:
                    status_report[index_name] = "already_exists"
                else:
                    status_report[index_name] = f"http_error_{e.code}"
            except Exception as exc:
                status_report[index_name] = f"error: {exc}"

        print(f"[Serving Layer - Index Init] {json.dumps(status_report, indent=2)}")
        return status_report

    def bulk_index(self, index_name: str, docs: List[Dict[str, object]]) -> int:
        """Bulk index documents into Elasticsearch using the _bulk NDJSON API."""
        if not docs or not self.is_reachable():
            return 0

        lines = []
        for doc in docs:
            payload = dict(doc)
            if "location" not in payload and "latitude" in payload and "longitude" in payload:
                payload["location"] = {
                    "lat": float(payload["latitude"]),
                    "lon": float(payload["longitude"]),
                }
            lines.append(json.dumps({"index": {"_index": index_name}}))
            lines.append(json.dumps(payload))

        body = ("\n".join(lines) + "\n").encode("utf-8")
        req = urllib.request.Request(
            f"{self.es_host}/_bulk",
            data=body,
            headers={"Content-Type": "application/x-ndjson"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10.0) as resp:
                if resp.status in (200, 201):
                    return len(docs)
        except Exception as exc:
            print(f"[Serving Layer] Bulk index warning for {index_name}: {exc}")
        return 0

    def sync_local_artifacts(
        self,
        clusters_json: str = "data/processed/station_clusters.json",
        baseline_json: str = "data/processed/station_baseline.json",
        alerts_json: str = "data/processed/realtime_alerts.json",
    ) -> Dict[str, int]:
        """Synchronize generated Batch & Speed Layer JSON artifacts into Elasticsearch."""
        counts = {"clusters": 0, "baseline": 0, "realtime_flow": 0, "alerts": 0}
        if os.path.exists(clusters_json):
            with open(clusters_json, "r", encoding="utf-8") as f:
                clusters = json.load(f)
            counts["clusters"] = self.bulk_index("mta-station-clusters", clusters)

        if os.path.exists(baseline_json):
            with open(baseline_json, "r", encoding="utf-8") as f:
                baseline = json.load(f)
            counts["baseline"] = self.bulk_index("mta-turnstile-baseline", baseline[:2000])

        if os.path.exists(alerts_json):
            with open(alerts_json, "r", encoding="utf-8") as f:
                rt = json.load(f)
            counts["realtime_flow"] = self.bulk_index(
                "mta-realtime-flow", rt.get("sample_realtime_views", [])
            )
            counts["alerts"] = self.bulk_index(
                "mta-congestion-alerts", rt.get("sample_critical_alerts", [])
            )

        print(f"[Serving Layer - Artifact Sync] {json.dumps(counts, indent=2)}")
        return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="MTA Elasticsearch Geo-Point Indexer")
    parser.add_argument(
        "--es-host",
        default="http://localhost:9200",
        help="Elasticsearch endpoint URL (default: http://localhost:9200)",
    )
    parser.add_argument(
        "--init-index",
        action="store_true",
        help="Initialize Elasticsearch indices with geo_point mappings",
    )
    parser.add_argument(
        "--sync-all",
        action="store_true",
        help="Sync Batch & Speed Layer JSON views into Elasticsearch",
    )
    args = parser.parse_args()

    client = ElasticsearchServingClient(es_host=args.es_host)
    if args.init_index or not args.sync_all:
        client.init_indices()
    if args.sync_all:
        client.sync_local_artifacts()


if __name__ == "__main__":
    main()
