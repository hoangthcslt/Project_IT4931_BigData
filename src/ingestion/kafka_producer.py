"""
Kafka Streaming Event Simulator for NYC Transit Turnstile Data.

Publishes micro-event passenger flow streams to Apache Kafka topic `mta-turnstile-stream`
at configurable throughput (e.g. 2,000 - 5,000 messages/second) in two modes:
  1. `--mode interpolate`: Micro-event Flow Interpolator (1-minute decomposed Poisson streams)
  2. `--mode replay`: Accelerated Snapshot Replay (sequential historical replay)
"""

import argparse
import glob
import json
import os
import sys
import time
from typing import Optional

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.ingestion.flow_interpolator import TurnstileFlowInterpolator


def create_kafka_producer(bootstrap_servers: str):
    """Initialize KafkaProducer with JSON serialization and high-throughput batching."""
    try:
        from kafka import KafkaProducer

        return KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            linger_ms=10,
            batch_size=65536,
            acks=1,
        )
    except Exception as exc:
        print(f"[KafkaProducer] Warning: Could not connect to Kafka at {bootstrap_servers} ({exc}).")
        return None


def run_producer(
    input_pattern: str,
    locations_csv: str,
    bootstrap_servers: str,
    topic: str,
    mode: str,
    rate: int,
    max_events: Optional[int] = None,
    dry_run: bool = False,
    jsonl_output: Optional[str] = None,
) -> dict:
    """Stream events from turnstile files into Kafka (or JSONL/dry-run sink)."""
    files = sorted(glob.glob(input_pattern))
    if not files:
        raise FileNotFoundError(f"No turnstile files matched pattern: {input_pattern}")

    interpolator = TurnstileFlowInterpolator(
        locations_csv=locations_csv,
        step_minutes=5 if mode == "interpolate" else 1,
        inject_rush_anomalies=True,
    )

    producer = None if dry_run else create_kafka_producer(bootstrap_servers)
    sink_file = open(jsonl_output, "w", encoding="utf-8") if jsonl_output else None

    print(
        f"[Event Simulator] Starting in mode='{mode}', target_rate={rate} msg/s, "
        f"topic='{topic}', broker='{bootstrap_servers}', dry_run={dry_run or (producer is None)}"
    )

    sent_count = 0
    start_time = time.perf_counter()
    window_start = start_time
    window_sent = 0

    try:
        for event in interpolator.stream_from_files(files, mode=mode):
            station_key = str(event["station_id"])
            if producer is not None:
                producer.send(topic, key=station_key, value=event)
            if sink_file is not None:
                sink_file.write(json.dumps(event) + "\n")

            sent_count += 1
            window_sent += 1

            # Rate limiting per 100ms slice to sustain target messages/sec smoothly
            if rate > 0 and window_sent >= max(1, rate // 10):
                elapsed = time.perf_counter() - window_start
                target_elapsed = window_sent / float(rate)
                if elapsed < target_elapsed:
                    time.sleep(target_elapsed - elapsed)
                window_start = time.perf_counter()
                window_sent = 0

            if sent_count % max(1000, rate) == 0:
                total_elapsed = max(0.001, time.perf_counter() - start_time)
                actual_rate = sent_count / total_elapsed
                print(
                    f"[Event Simulator] Published {sent_count:,} events | "
                    f"Throughput: {actual_rate:,.1f} events/sec | "
                    f"Latest station: {event['stop_name']} ({event['event_timestamp']})"
                )

            if max_events and sent_count >= max_events:
                break
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
        if sink_file is not None:
            sink_file.close()

    total_time = max(0.001, time.perf_counter() - start_time)
    summary = {
        "events_published": sent_count,
        "elapsed_seconds": round(total_time, 3),
        "avg_throughput_msg_per_sec": round(sent_count / total_time, 1),
        "mode": mode,
        "topic": topic,
    }
    print(f"[Event Simulator Summary] {json.dumps(summary, indent=2)}")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="MTA Turnstile Kafka Streaming Producer")
    parser.add_argument(
        "--input",
        default="data/archive/TurnstileData/turnstile_*.txt",
        help="Input glob pattern for turnstile files",
    )
    parser.add_argument(
        "--locations",
        default="data/archive/MTA_StationLocations.csv",
        help="Path to MTA_StationLocations.csv",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topic",
        default="mta-turnstile-stream",
        help="Kafka topic name (default: mta-turnstile-stream)",
    )
    parser.add_argument(
        "--mode",
        choices=["interpolate", "replay"],
        default="interpolate",
        help="Streaming mode: 'interpolate' (1-min micro-events) or 'replay' (snapshot replay)",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=3000,
        help="Target throughput in messages/second (default: 3000)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after publishing N events (useful for benchmark/testing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run simulator without requiring a live Kafka broker",
    )
    parser.add_argument(
        "--jsonl-output",
        default=None,
        help="Optional local JSONL file to tee simulated events for offline streaming tests",
    )
    args = parser.parse_args()

    run_producer(
        input_pattern=args.input,
        locations_csv=args.locations,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        mode=args.mode,
        rate=args.rate,
        max_events=args.max_events,
        dry_run=args.dry_run,
        jsonl_output=args.jsonl_output,
    )


if __name__ == "__main__":
    main()
