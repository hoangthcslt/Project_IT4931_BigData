"""
Micro-event Flow Interpolator for 4-Hour Cumulative Turnstile Logs.

Addresses Challenge #3 in docs/overview.md:
Decomposes 4-hour cumulative turnstile intervals (Delta Entries & Delta Exits) into
1-minute micro-event streams weighted by empirical NYC Subway diurnal rush-hour profiles
and Poisson-like stochastic variations.
"""

import csv
import math
import random
from datetime import datetime, timedelta
from typing import Dict, Iterator, List, Optional, Tuple

from src.batch.clean_turnstile import MAX_RATE_PER_HOUR
from src.batch.entity_resolution import StationEntityResolver

# Relative diurnal intensity weights for each hour of day (0..23) in NYC Subway
HOURLY_RUSH_WEIGHTS: Dict[int, float] = {
    0: 0.25,
    1: 0.15,
    2: 0.10,
    3: 0.12,
    4: 0.25,
    5: 0.60,
    6: 1.25,
    7: 2.10,
    8: 2.65,  # Morning peak
    9: 1.85,
    10: 1.15,
    11: 1.05,
    12: 1.15,
    13: 1.20,
    14: 1.30,
    15: 1.60,
    16: 2.15,
    17: 2.55,  # Evening peak
    18: 2.20,
    19: 1.45,
    20: 1.00,
    21: 0.80,
    22: 0.60,
    23: 0.40,
}


def poisson_sample(lam: float, rng: random.Random) -> int:
    """Fast Poisson random variate generator (Knuth for small lambda, normal approx for large)."""
    if lam <= 0.0:
        return 0
    if lam < 30.0:
        L = math.exp(-lam)
        k = 0
        p = 1.0
        while p > L:
            k += 1
            p *= rng.random()
        return k - 1
    else:
        val = rng.gauss(lam, math.sqrt(lam))
        return max(0, int(round(val)))


class TurnstileFlowInterpolator:
    """
    Converts 4-hour turnstile cumulative logs into fine-grained 1-minute streaming micro-events
    suitable for high-throughput Kafka ingestion and 15-minute Sliding Window processing.
    """

    def __init__(
        self,
        locations_csv: str = "data/archive/MTA_StationLocations.csv",
        step_minutes: int = 1,
        seed: int = 42,
        inject_rush_anomalies: bool = True,
    ):
        self.resolver = StationEntityResolver(locations_csv)
        self.step_minutes = max(1, step_minutes)
        self.rng = random.Random(seed)
        self.inject_rush_anomalies = inject_rush_anomalies

    def decompose_interval(
        self,
        ca: str,
        unit: str,
        scp: str,
        station_meta: Dict[str, object],
        start_ts: datetime,
        end_ts: datetime,
        delta_entries: int,
        delta_exits: int,
    ) -> List[Dict[str, object]]:
        """
        Split a single (start_ts, end_ts) turnstile delta into `step_minutes` micro-events
        weighted by time-of-day rush hour curves.
        """
        total_minutes = int((end_ts - start_ts).total_seconds() // 60)
        if total_minutes <= 0 or (delta_entries == 0 and delta_exits == 0):
            return []

        num_steps = max(1, total_minutes // self.step_minutes)
        step_timestamps: List[datetime] = []
        raw_weights: List[float] = []

        for step_idx in range(num_steps):
            cur_ts = start_ts + timedelta(minutes=(step_idx + 1) * self.step_minutes)
            step_timestamps.append(cur_ts)
            h_weight = HOURLY_RUSH_WEIGHTS.get(cur_ts.hour, 1.0)
            # Subtle intra-hour wave peaking around :15 and :45 when trains discharge passengers
            minute_wave = 1.0 + 0.15 * math.sin(2.0 * math.pi * cur_ts.minute / 30.0)
            raw_weights.append(h_weight * minute_wave)

        weight_sum = sum(raw_weights) or 1.0
        norm_weights = [w / weight_sum for w in raw_weights]

        events: List[Dict[str, object]] = []
        for cur_ts, w in zip(step_timestamps, norm_weights):
            e_sub = poisson_sample(delta_entries * w, self.rng)
            x_sub = poisson_sample(delta_exits * w, self.rng)
            if e_sub == 0 and x_sub == 0:
                continue

            # Optional rush-hour overcrowding surge on major Manhattan hubs for realistic alert testing
            if (
                self.inject_rush_anomalies
                and station_meta.get("borough") == "M"
                and cur_ts.hour in (8, 17, 18)
                and self.rng.random() < 0.03
            ):
                e_sub = int(e_sub * 2.8)
                x_sub = int(x_sub * 2.8)

            dow = (cur_ts.isoweekday() % 7) + 1
            events.append(
                {
                    "event_id": f"{station_meta['station_id']}_{scp}_{int(cur_ts.timestamp())}",
                    "event_timestamp": cur_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "station_id": str(station_meta["station_id"]),
                    "complex_id": str(station_meta["complex_id"]),
                    "stop_name": station_meta["stop_name"],
                    "borough": station_meta["borough"],
                    "daytime_routes": station_meta["daytime_routes"],
                    "latitude": float(station_meta["latitude"]),
                    "longitude": float(station_meta["longitude"]),
                    "ca": ca,
                    "unit": unit,
                    "scp": scp,
                    "entries_delta": e_sub,
                    "exits_delta": x_sub,
                    "passenger_flow": e_sub + x_sub,
                    "day_of_week": dow,
                    "hour_bucket": cur_ts.hour,
                    "mode": "interpolated_micro_event",
                }
            )
        return events

    def stream_from_files(
        self,
        file_paths: List[str],
        mode: str = "interpolate",
    ) -> Iterator[Dict[str, object]]:
        """
        Yield streaming turnstile events from raw turnstile files in either:
          - 'interpolate': Micro-event Flow Interpolation (1-minute decomposed events)
          - 'replay': Accelerated Snapshot Replay (4-hour interval delta events)
        """
        prev_state: Dict[Tuple[str, str, str, str], Tuple[datetime, int, int]] = {}

        for fpath in file_paths:
            with open(fpath, mode="r", encoding="utf-8", errors="ignore") as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if len(row) < 11:
                        continue
                    ca = row[0].strip()
                    unit = row[1].strip()
                    scp = row[2].strip()
                    raw_station = row[3].strip()
                    raw_linename = row[4].strip()
                    raw_division = row[5].strip()
                    date_str = row[6].strip()
                    time_str = row[7].strip()

                    resolved = self.resolver.resolve(raw_station, raw_linename, raw_division)
                    if resolved is None:
                        continue

                    try:
                        entries = int(row[9].strip())
                        exits = int(row[10].strip())
                        ts = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%Y %H:%M:%S")
                    except ValueError:
                        continue

                    key = (ca, unit, scp, raw_station)
                    if key in prev_state:
                        prev_ts, prev_e, prev_x = prev_state[key]
                        dh = (ts - prev_ts).total_seconds() / 3600.0
                        de = entries - prev_e
                        dx = exits - prev_x
                        max_allowed = MAX_RATE_PER_HOUR * max(dh, 1.0)

                        if 0 < dh <= 24.0 and 0 <= de <= max_allowed and 0 <= dx <= max_allowed:
                            if mode == "replay":
                                dow = (ts.isoweekday() % 7) + 1
                                yield {
                                    "event_id": f"{resolved['station_id']}_{scp}_{int(ts.timestamp())}",
                                    "event_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                                    "station_id": str(resolved["station_id"]),
                                    "complex_id": str(resolved["complex_id"]),
                                    "stop_name": resolved["stop_name"],
                                    "borough": resolved["borough"],
                                    "daytime_routes": resolved["daytime_routes"],
                                    "latitude": float(resolved["latitude"]),
                                    "longitude": float(resolved["longitude"]),
                                    "ca": ca,
                                    "unit": unit,
                                    "scp": scp,
                                    "entries_delta": de,
                                    "exits_delta": dx,
                                    "passenger_flow": de + dx,
                                    "day_of_week": dow,
                                    "hour_bucket": ts.hour,
                                    "mode": "accelerated_snapshot_replay",
                                }
                            else:
                                for micro_ev in self.decompose_interval(
                                    ca, unit, scp, resolved, prev_ts, ts, de, dx
                                ):
                                    yield micro_ev

                    prev_state[key] = (ts, entries, exits)
