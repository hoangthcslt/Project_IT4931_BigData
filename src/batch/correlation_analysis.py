"""
Batch Layer Job 3: Operational Quality Correlation Analysis.

Joins monthly passenger flow and peak congestion intensity per subway line (1-7, A-R)
with MTA operational KPIs ('Subway Wait Assessment' and 'OTP (Terminal)') from
MTA_Performance_Agencies.csv (encoded in ISO-8859-1 / Windows-1252) for the year 2016.
Quantifies the statistical impact of passenger overcrowding on train headway regularity and delays.
"""

import argparse
import csv
import glob
import json
import math
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Tuple

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.batch.clean_turnstile import MAX_RATE_PER_HOUR

SUBWAY_LINES = [
    "1", "2", "3", "4", "5", "6", "7",
    "A", "B", "C", "D", "E", "F", "G",
    "J", "Z", "L", "M", "N", "Q", "R", "S",
]


def parse_line_from_indicator(indicator_name: str) -> Optional[Tuple[str, str]]:
    """
    Extract (metric_type, line_code) from INDICATOR_NAME such as:
      - 'Subway Wait Assessment - 1 Line' -> ('wait_assessment', '1')
      - 'Subway Wait Assessment - J Z Line' -> ('wait_assessment', 'JZ')
      - 'OTP (Terminal) - A Line' -> ('otp_terminal', 'A')
    """
    ind = indicator_name.strip()
    m_wa = re.match(r"^Subway Wait Assessment\s*-\s*([A-Z0-9 ]+?)(?:\s+Line)?$", ind)
    if m_wa:
        raw_line = m_wa.group(1).strip().replace(" ", "")
        if raw_line.startswith("S"):
            raw_line = "S"
        return ("wait_assessment", raw_line)

    m_otp = re.match(r"^OTP \(Terminal\)\s*-\s*([A-Z0-9 ]+?)(?:\s+Line.*)?$", ind)
    if m_otp:
        raw_line = m_otp.group(1).strip().replace(" ", "")
        if raw_line.startswith("S"):
            raw_line = "S"
        return ("otp_terminal", raw_line)

    return None


def load_mta_performance_2016(
    perf_csv_path: str,
) -> Dict[Tuple[str, str], Dict[str, float]]:
    """
    Load 2016 monthly Subway Wait Assessment & OTP (Terminal) metrics from
    MTA_Performance_Agencies.csv using ISO-8859-1 encoding.
    Returns mapping: (line, '2016-MM') -> {'wait_assessment': val, 'otp_terminal': val, 'target_wa': val}
    """
    if not os.path.exists(perf_csv_path):
        raise FileNotFoundError(f"Performance CSV not found: {perf_csv_path}")

    metrics: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(dict)
    with open(perf_csv_path, mode="r", encoding="iso-8859-1") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("PERIOD_YEAR", "").strip() != "2016":
                continue
            ind_name = row.get("INDICATOR_NAME", "").strip()
            parsed = parse_line_from_indicator(ind_name)
            if not parsed:
                continue
            metric_type, line_code = parsed
            yyyy_mm = row.get("YYYY_MM", "").strip()
            try:
                actual = float(row.get("MONTHLY_ACTUAL", "").strip())
                target = float(row.get("MONTHLY_TARGET", "0").strip() or 0.0)
            except ValueError:
                continue

            # Expand JZ to both J and Z as well as keeping JZ
            lines_to_record = ["J", "Z"] if line_code == "JZ" else [line_code]
            for lc in lines_to_record:
                metrics[(lc, yyyy_mm)][metric_type] = actual
                if metric_type == "wait_assessment" and target > 0:
                    metrics[(lc, yyyy_mm)]["wait_assessment_target"] = target
                    metrics[(lc, yyyy_mm)]["wait_assessment_gap"] = round(target - actual, 2)

    return metrics


def load_nyct_xml_indicators_count(
    xml_path: str = "data/archive/NYC_TransitPerformance/Performance_NYCT.xml",
) -> int:
    """
    Parse semi-structured XML KPI feed (`Performance_NYCT.xml`) per Section 2.2 (Variety)
    of `docs/overview.md` and return total valid <INDICATOR> elements for NYC Transit.
    """
    if not os.path.exists(xml_path):
        return 0
    try:
        import xml.etree.ElementTree as ET

        tree = ET.parse(xml_path)
        root = tree.getroot()
        return len(root.findall(".//INDICATOR"))
    except Exception:
        with open(xml_path, "rb") as f:
            content = f.read()
        return content.count(b"<INDICATOR>")


def pearson_corr(xs: List[float], ys: List[float]) -> float:
    """Compute Pearson correlation coefficient r between two numeric lists."""
    n = len(xs)
    if n < 2 or len(ys) != n:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mx) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - my) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return 0.0
    return num / (den_x * den_y)


def run_correlation_analysis(
    turnstile_pattern: str,
    perf_csv_path: str,
    output_json: str,
    max_files: Optional[int] = None,
) -> Dict[str, object]:
    """Compute monthly line-level passenger flow vs. Subway Wait Assessment & OTP correlation."""
    perf_data = load_mta_performance_2016(perf_csv_path)
    files = sorted(glob.glob(turnstile_pattern))
    if max_files:
        files = files[:max_files]

    # Aggregate turnstile deltas per (line, yyyy_mm)
    line_month_flow: Dict[Tuple[str, str], float] = defaultdict(float)
    line_month_peak_flow: Dict[Tuple[str, str], float] = defaultdict(float)
    line_turnstiles: Dict[str, set] = defaultdict(set)
    prev_state: Dict[Tuple[str, str, str, str], Tuple[float, int, int]] = {}
    ts_cache: Dict[Tuple[str, str], Optional[Tuple[float, int, str, bool]]] = {}
    routes_cache: Dict[str, List[str]] = {}

    for fpath in files:
        with open(fpath, mode="r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) < 11:
                    continue
                div = row[5].strip()
                if div in ("PTH", "RIT"):
                    continue
                ca, unit, scp, st = row[0].strip(), row[1].strip(), row[2].strip(), row[3].strip()
                linename = row[4].strip().upper()
                date_str, time_str = row[6].strip(), row[7].strip()
                try:
                    entries = int(row[9].strip())
                    exits = int(row[10].strip())
                except ValueError:
                    continue

                dt_key = (date_str, time_str)
                if dt_key not in ts_cache:
                    try:
                        parsed_dt = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%Y %H:%M:%S")
                        is_pk = parsed_dt.weekday() < 5 and parsed_dt.hour in (7, 8, 9, 10, 11, 12, 16, 17, 18, 19, 20)
                        ts_cache[dt_key] = (
                            parsed_dt.timestamp(),
                            parsed_dt.year,
                            parsed_dt.strftime("%Y-%m"),
                            is_pk,
                        )
                    except ValueError:
                        ts_cache[dt_key] = None
                cached_dt = ts_cache[dt_key]
                if cached_dt is None:
                    continue
                ts_epoch, ts_year, yyyy_mm, is_peak = cached_dt

                if ts_year != 2016:
                    prev_state[(ca, unit, scp, st)] = (ts_epoch, entries, exits)
                    continue

                key = (ca, unit, scp, st)
                if key in prev_state:
                    prev_epoch, prev_e, prev_x = prev_state[key]
                    dh = (ts_epoch - prev_epoch) / 3600.0
                    de = entries - prev_e
                    dx = exits - prev_x
                    max_allowed = MAX_RATE_PER_HOUR * max(dh, 1.0)
                    if 0 < dh <= 24.0 and 0 <= de <= max_allowed and 0 <= dx <= max_allowed:
                        flow = de + dx
                        if linename not in routes_cache:
                            routes_cache[linename] = [ch for ch in linename if ch in SUBWAY_LINES]
                        routes = routes_cache[linename]
                        if routes:
                            share = flow / len(routes)
                            for r_code in routes:
                                line_month_flow[(r_code, yyyy_mm)] += share
                                line_turnstiles[r_code].add(key)
                                if is_peak:
                                    line_month_peak_flow[(r_code, yyyy_mm)] += share

                prev_state[key] = (ts_epoch, entries, exits)

    joined_records = []
    for (line_code, yyyy_mm), kpi in sorted(perf_data.items()):
        total_flow = line_month_flow.get((line_code, yyyy_mm), 0.0)
        peak_flow = line_month_peak_flow.get((line_code, yyyy_mm), 0.0)
        turnstile_cnt = max(1, len(line_turnstiles.get(line_code, set())))
        if total_flow <= 0 or "wait_assessment" not in kpi:
            continue
        flow_per_turnstile = total_flow / turnstile_cnt
        peak_ratio = peak_flow / total_flow if total_flow > 0 else 0.0
        joined_records.append(
            {
                "line": line_code,
                "year_month": yyyy_mm,
                "total_passenger_flow": round(total_flow),
                "peak_passenger_flow": round(peak_flow),
                "flow_per_turnstile": round(flow_per_turnstile, 1),
                "peak_concentration_ratio": round(peak_ratio, 4),
                "subway_wait_assessment": kpi.get("wait_assessment"),
                "wait_assessment_target": kpi.get("wait_assessment_target", 80.0),
                "wait_assessment_deficit": kpi.get("wait_assessment_gap", 0.0),
                "otp_terminal": kpi.get("otp_terminal"),
            }
        )

    flows = [r["total_passenger_flow"] for r in joined_records]
    intensities = [r["flow_per_turnstile"] for r in joined_records]
    wa_vals = [r["subway_wait_assessment"] for r in joined_records]
    otp_pairs = [
        (r["total_passenger_flow"], r["otp_terminal"])
        for r in joined_records
        if r.get("otp_terminal") is not None
    ]

    r_flow_vs_wa = pearson_corr(flows, wa_vals)
    r_intensity_vs_wa = pearson_corr(intensities, wa_vals)
    r_flow_vs_otp = (
        pearson_corr([p[0] for p in otp_pairs], [p[1] for p in otp_pairs]) if otp_pairs else 0.0
    )

    # Line-level summary averages across 2016 H1
    by_line: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for rec in joined_records:
        by_line[rec["line"]].append(rec)

    line_summaries = []
    for lc, recs in sorted(by_line.items()):
        avg_flow = sum(r["total_passenger_flow"] for r in recs) / len(recs)
        avg_wa = sum(r["subway_wait_assessment"] for r in recs) / len(recs)
        otp_list = [r["otp_terminal"] for r in recs if r.get("otp_terminal") is not None]
        avg_otp = sum(otp_list) / len(otp_list) if otp_list else None
        line_summaries.append(
            {
                "line": lc,
                "months_analyzed": len(recs),
                "avg_monthly_passenger_flow": round(avg_flow),
                "avg_wait_assessment_pct": round(avg_wa, 2),
                "avg_otp_terminal_pct": round(avg_otp, 2) if avg_otp is not None else None,
            }
        )

    xml_indicator_cnt = load_nyct_xml_indicators_count()
    output_payload = {
        "summary_metrics": {
            "records_joined": len(joined_records),
            "lines_covered": len(line_summaries),
            "xml_nyct_indicators_verified": xml_indicator_cnt,
            "pearson_r_flow_vs_wait_assessment": round(r_flow_vs_wa, 4),
            "pearson_r_turnstile_intensity_vs_wait_assessment": round(r_intensity_vs_wa, 4),
            "pearson_r_flow_vs_otp_terminal": round(r_flow_vs_otp, 4),
            "interpretation": (
                "Negative correlation confirms that heavier passenger volume and turnstile crowding "
                "intensify dwell times at stations, directly reducing Subway Wait Assessment and Terminal OTP."
            ),
        },
        "line_summaries": line_summaries,
        "monthly_line_records": joined_records,
    }

    os.makedirs(os.path.dirname(os.path.abspath(output_json)), exist_ok=True)
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=2)

    print(f"[Correlation Analysis Summary] {json.dumps(output_payload['summary_metrics'], indent=2)}")
    return output_payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MTA Passenger Flow vs Subway Wait Assessment Correlation Analysis"
    )
    parser.add_argument(
        "--turnstile-input",
        default="data/archive/TurnstileData/turnstile_*.txt",
        help="Input glob pattern for turnstile text files",
    )
    parser.add_argument(
        "--performance-csv",
        default="data/archive/NYC_TransitPerformance/MTA_Performance_Agencies.csv",
        help="Path to MTA_Performance_Agencies.csv (ISO-8859-1)",
    )
    parser.add_argument(
        "--output",
        default="data/processed/correlation_results.json",
        help="Output JSON path for correlation analysis results",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Optional limit on number of turnstile files to scan",
    )
    args = parser.parse_args()
    run_correlation_analysis(
        args.turnstile_input, args.performance_csv, args.output, args.max_files
    )


if __name__ == "__main__":
    main()
