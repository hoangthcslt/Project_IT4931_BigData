"""
Z-Score Overcrowding Anomaly Detector for NYC Subway Speed Layer.

Implements the Lambda Architecture Speed/Batch join formula:
    Z-Score = (FlowRate_15m - Mean_15m) / Std_15m
Triggers `CRITICAL_OVERCROWDING_ALERT` whenever Z-Score > 3.0.
"""

import json
import os
from typing import Dict, List, Optional, Tuple

CRITICAL_Z_THRESHOLD = 3.0
HIGH_Z_THRESHOLD = 2.0
MODERATE_Z_THRESHOLD = 1.0


def classify_congestion_status(z_score: float) -> Tuple[str, str, bool]:
    """
    Classify station crowding state based on Z-Score.
    Returns (status_code, ui_color, is_critical_alert).
    """
    if z_score > CRITICAL_Z_THRESHOLD:
        return ("CRITICAL_OVERCROWDING_ALERT", "RED_BLINKING", True)
    if z_score > HIGH_Z_THRESHOLD:
        return ("HIGH_CONGESTION", "ORANGE", False)
    if z_score > MODERATE_Z_THRESHOLD:
        return ("MODERATE_CROWDING", "YELLOW", False)
    return ("NORMAL", "GREEN", False)


class ZScoreAnomalyDetector:
    """Evaluates real-time 15-minute sliding window passenger flow against historical baselines."""

    def __init__(
        self,
        baseline_json_path: str = "data/processed/station_baseline.json",
        z_threshold: float = CRITICAL_Z_THRESHOLD,
    ):
        self.baseline_json_path = baseline_json_path
        self.z_threshold = z_threshold
        # Key: (station_id, day_of_week, hour_bucket) -> (mean_15m, std_15m)
        self._lookup: Dict[Tuple[str, int, int], Tuple[float, float]] = {}
        # Station fallback: station_id -> (mean_15m, std_15m)
        self._station_fallback: Dict[str, Tuple[float, float]] = {}
        self._load_baseline()

    def _load_baseline(self) -> None:
        if not os.path.exists(self.baseline_json_path):
            return
        with open(self.baseline_json_path, "r", encoding="utf-8") as f:
            rows = json.load(f)

        station_accum: Dict[str, List[Tuple[float, float]]] = {}
        for r in rows:
            st_id = str(r["station_id"])
            dow = int(r["day_of_week"])
            hb = int(r["hour_bucket"])
            mean_val = float(r.get("mean_flow_15m", 50.0))
            std_val = max(float(r.get("std_flow_15m", 15.0)), 5.0)
            self._lookup[(st_id, dow, hb)] = (mean_val, std_val)
            station_accum.setdefault(st_id, []).append((mean_val, std_val))

        for st_id, pairs in station_accum.items():
            avg_m = sum(p[0] for p in pairs) / len(pairs)
            avg_s = sum(p[1] for p in pairs) / len(pairs)
            self._station_fallback[st_id] = (avg_m, max(avg_s, 5.0))

    def get_baseline(self, station_id: str, day_of_week: int, hour_bucket: int) -> Tuple[float, float]:
        """Retrieve (mean_flow_15m, std_flow_15m) for a given station and temporal slot."""
        st_id = str(station_id)
        key = (st_id, int(day_of_week), int(hour_bucket))
        if key in self._lookup:
            return self._lookup[key]
        # Try nearest 4-hour bucket boundary
        hb_aligned = (int(hour_bucket) // 4) * 4
        if (st_id, int(day_of_week), hb_aligned) in self._lookup:
            return self._lookup[(st_id, int(day_of_week), hb_aligned)]
        if st_id in self._station_fallback:
            return self._station_fallback[st_id]
        return (80.0, 25.0)

    def evaluate_window(
        self,
        station_id: str,
        stop_name: str,
        borough: str,
        daytime_routes: str,
        latitude: float,
        longitude: float,
        window_start: str,
        window_end: str,
        entries_15m: int,
        exits_15m: int,
        day_of_week: int,
        hour_bucket: int,
    ) -> Dict[str, object]:
        """Compute Z-Score and anomaly alert status for a 15-minute sliding window observation."""
        flow_rate = float(entries_15m + exits_15m)
        mean_15m, std_15m = self.get_baseline(station_id, day_of_week, hour_bucket)
        z_score = (flow_rate - mean_15m) / max(std_15m, 1.0)
        status_code, ui_color, is_critical = classify_congestion_status(z_score)

        return {
            "station_id": str(station_id),
            "stop_name": stop_name,
            "borough": borough,
            "daytime_routes": daytime_routes,
            "latitude": float(latitude),
            "longitude": float(longitude),
            "location": {"lat": float(latitude), "lon": float(longitude)},
            "window_start": window_start,
            "window_end": window_end,
            "entries_15m": int(entries_15m),
            "exits_15m": int(exits_15m),
            "flow_rate_15m": round(flow_rate, 1),
            "baseline_mean_15m": round(mean_15m, 2),
            "baseline_std_15m": round(std_15m, 2),
            "z_score": round(z_score, 3),
            "alert_status": status_code,
            "heatmap_color": ui_color,
            "is_critical_alert": is_critical,
            "day_of_week": int(day_of_week),
            "hour_bucket": int(hour_bucket),
        }
