#!/usr/bin/env python3
"""Analyse a Nexus N3 session ZIP.

Checks:
- node diagnostics and lifecycle status
- coordinated official-start timing
- raw CSV sample counts, duration and observed rate
- timestamp cadence, gap events and estimated dropped samples
- gateway/parser/transport drop counters
- raw write failures and diagnostic errors
- presence/count of computed outputs

Usage:
    python analyse_nexus_n3_session.py session.zip
    python analyse_nexus_n3_session.py session.zip --json report.json
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import tempfile
import zipfile
from collections import Counter
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_int(value: str) -> int:
    return int(float(value))


def analyse_csv(path: Path) -> dict[str, Any]:
    timestamps: list[int] = []
    sampling_rate = None
    address = None
    location = None
    sensor_type = None

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            timestamps.append(parse_int(row["timestamp"]))
            if sampling_rate is None:
                sampling_rate = float(row["sampling_rate"])
                address = row.get("address")
                location = row.get("location")
                sensor_type = row.get("sensor_type")

    if not timestamps:
        return {"file": str(path), "samples": 0}

    intervals = [b - a for a, b in zip(timestamps, timestamps[1:])]
    nominal_us = 1_000_000.0 / sampling_rate if sampling_rate else math.nan
    gap_threshold_us = nominal_us * 1.5
    gaps = [dt for dt in intervals if dt > gap_threshold_us]
    estimated_dropped = sum(max(round(dt / nominal_us) - 1, 0) for dt in gaps)

    duration_s = (
        (timestamps[-1] - timestamps[0]) / 1_000_000.0
        if len(timestamps) > 1 else 0.0
    )
    observed_rate = (
        (len(timestamps) - 1) / duration_s if duration_s > 0 else None
    )

    sorted_intervals = sorted(intervals)
    def percentile(values: list[int], pct: float) -> float | None:
        if not values:
            return None
        pos = (len(values) - 1) * pct
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return float(values[lo])
        return values[lo] + (values[hi] - values[lo]) * (pos - lo)

    return {
        "file": str(path),
        "address": address,
        "location": location,
        "sensor_type": sensor_type,
        "declared_rate_hz": sampling_rate,
        "samples": len(timestamps),
        "duration_s": round(duration_s, 6),
        "observed_rate_hz": round(observed_rate, 6) if observed_rate else None,
        "first_timestamp": timestamps[0],
        "last_timestamp": timestamps[-1],
        "nominal_interval_us": round(nominal_us, 3),
        "median_interval_us": statistics.median(intervals) if intervals else None,
        "min_interval_us": min(intervals) if intervals else None,
        "max_interval_us": max(intervals) if intervals else None,
        "p99_interval_us": round(percentile(sorted_intervals, 0.99), 3)
            if intervals else None,
        "gap_events": len(gaps),
        "estimated_dropped_samples": int(estimated_dropped),
        "gap_interval_counts": dict(Counter(gaps).most_common(20)),
    }


def collect_zero_sensitive_counters(obj: Any, prefix: str = "") -> dict[str, Any]:
    """Collect diagnostic counters whose names indicate drops/failures/errors."""
    found: dict[str, Any] = {}
    keywords = ("drop", "fail", "error", "checksum", "resync")
    if isinstance(obj, dict):
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            if (
                isinstance(value, (int, float))
                and any(k in key.lower() for k in keywords)
            ):
                found[path] = value
            found.update(collect_zero_sensitive_counters(value, path))
    elif isinstance(obj, list):
        for idx, value in enumerate(obj):
            found.update(collect_zero_sensitive_counters(value, f"{prefix}[{idx}]"))
    return found


def analyse_diagnostic(path: Path) -> dict[str, Any]:
    d = load_json(path)
    events = d.get("lifecycle_events", [])
    official_event = next(
        (e for e in events if e.get("type") == "stream_official_started"), None
    )
    ready_event = next(
        (e for e in events if e.get("type") == "stream_ready_for_official"), None
    )
    stop_event = next(
        (e for e in events if e.get("type") == "stream_stopped"), None
    )

    timing = (
        official_event.get("payload", {}).get("official_start_timing", {})
        if official_event else {}
    )
    ready_sensors = (
        ready_event.get("payload", {}).get("sensors", [])
        if ready_event else []
    )

    latest_gateway = d.get("latest_gateway_diagnostics", {})
    counters = collect_zero_sensitive_counters(latest_gateway)
    nonzero_counters = {k: v for k, v in counters.items() if v != 0}

    return {
        "file": str(path),
        "node_id": d.get("node_id"),
        "status": d.get("status"),
        "official_stream": d.get("official_stream"),
        "subject_ids": d.get("subject_ids", []),
        "started_at": d.get("started_at"),
        "official_stream_started_at": d.get("official_stream_started_at"),
        "finalized_at": d.get("finalized_at"),
        "errors": d.get("errors", []),
        "raw_write_failures": d.get("raw_write_failures", {}),
        "partial_markers": d.get("partial_markers", []),
        "stop_summary": d.get("stop_summary", {}),
        "drain_summary": d.get("drain_summary", {}),
        "lifecycle": [
            {"timestamp": e.get("timestamp"), "type": e.get("type")}
            for e in events
        ],
        "startup_sensors": ready_sensors,
        "official_start_timing": timing,
        "gateway_nonzero_error_drop_counters": nonzero_counters,
        "gateway_checked_counter_count": len(counters),
        "stream_stopped_at": stop_event.get("timestamp") if stop_event else None,
    }


def analyse_session(zip_path: Path) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="nexus_n3_session_") as td:
        root = Path(td)
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(root)

        diagnostics = [
            analyse_diagnostic(p)
            for p in sorted(root.glob("*-diagnostics/session_diagnostics.json"))
        ]
        raw = [
            analyse_csv(p)
            for p in sorted(root.glob("subjects/*/activities/*/raw/*.csv"))
        ]

        for item in raw:
            parts = Path(item["file"]).parts
            if "subjects" in parts:
                item["subject_id"] = parts[parts.index("subjects") + 1]
            item["file"] = str(Path(item["file"]).relative_to(root))

        for item in diagnostics:
            item["file"] = str(Path(item["file"]).relative_to(root))

        computed: dict[str, int] = {}
        for category in ("real_time", "intermediate", "consolidated"):
            files = list(root.glob(
                f"subjects/*/activities/*/computed/{category}/**/*.ndjson"
            ))
            computed[category] = len(files)

        start_completions = []
        for d in diagnostics:
            timing = d.get("official_start_timing", {})
            utc = timing.get("node_activation_completed_utc_ns")
            if utc is not None:
                start_completions.append((d["node_id"], int(utc)))

        activation_completion_spread_ms = None
        if len(start_completions) >= 2:
            ns = [x[1] for x in start_completions]
            activation_completion_spread_ms = (max(ns) - min(ns)) / 1_000_000.0

        durations = [r["duration_s"] for r in raw if r.get("duration_s") is not None]
        raw_duration_spread_s = max(durations) - min(durations) if durations else None

        return {
            "session_zip": zip_path.name,
            "node_count": len(diagnostics),
            "subject_count": len({r.get("subject_id") for r in raw}),
            "raw_stream_count": len(raw),
            "all_node_status_ok": all(d.get("status") == "ok" for d in diagnostics),
            "all_official_streams_passed": all(
                d.get("official_stream") == "passed" for d in diagnostics
            ),
            "all_errors_empty": all(not d.get("errors") for d in diagnostics),
            "all_raw_write_failures_empty": all(
                not d.get("raw_write_failures") for d in diagnostics
            ),
            "all_gateway_error_drop_counters_zero": all(
                not d.get("gateway_nonzero_error_drop_counters")
                for d in diagnostics
            ),
            "total_gap_events": sum(r.get("gap_events", 0) for r in raw),
            "total_estimated_dropped_samples": sum(
                r.get("estimated_dropped_samples", 0) for r in raw
            ),
            "official_activation_completion_spread_ms":
                round(activation_completion_spread_ms, 3)
                if activation_completion_spread_ms is not None else None,
            "raw_duration_spread_s":
                round(raw_duration_spread_s, 6)
                if raw_duration_spread_s is not None else None,
            "computed_output_files": computed,
            "diagnostics": diagnostics,
            "raw_streams": raw,
        }


def print_report(report: dict[str, Any]) -> None:
    print(f"Session: {report['session_zip']}")
    print(
        f"Nodes: {report['node_count']} | Subjects: {report['subject_count']} | "
        f"Raw streams: {report['raw_stream_count']}"
    )
    print(
        f"Status OK: {report['all_node_status_ok']} | "
        f"Official streams passed: {report['all_official_streams_passed']}"
    )
    print(
        f"Gap events: {report['total_gap_events']} | "
        f"Estimated dropped: {report['total_estimated_dropped_samples']}"
    )
    print(
        "Official activation completion spread: "
        f"{report['official_activation_completion_spread_ms']} ms"
    )
    print(f"Raw duration spread: {report['raw_duration_spread_s']} s")

    print("\nRaw streams")
    print("-" * 112)
    print(
        f"{'Subject':18} {'Location':13} {'Samples':>8} {'Dur(s)':>9} "
        f"{'Hz':>9} {'Median us':>11} {'Max us':>9} {'Gaps':>6} {'Drop est':>9}"
    )
    for r in report["raw_streams"]:
        print(
            f"{r.get('subject_id',''):18} {r.get('location',''):13} "
            f"{r.get('samples',0):8d} {r.get('duration_s',0):9.3f} "
            f"{r.get('observed_rate_hz',0):9.4f} "
            f"{r.get('median_interval_us',0):11.1f} "
            f"{r.get('max_interval_us',0):9d} "
            f"{r.get('gap_events',0):6d} "
            f"{r.get('estimated_dropped_samples',0):9d}"
        )

    print("\nNodes")
    print("-" * 112)
    for d in report["diagnostics"]:
        timing = d.get("official_start_timing", {})
        print(
            f"{d.get('node_id')}: status={d.get('status')}, "
            f"official={d.get('official_stream')}, "
            f"preparation="
            f"{timing.get('node_persistence_preparation_duration_ms')} ms, "
            f"activation={timing.get('node_activation_duration_ms')} ms, "
            f"errors={len(d.get('errors', []))}, "
            f"write_failures={len(d.get('raw_write_failures', {}))}, "
            f"nonzero_gateway_counters="
            f"{len(d.get('gateway_nonzero_error_drop_counters', {}))}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyse a Nexus N3 session ZIP.")
    parser.add_argument("session_zip", type=Path)
    parser.add_argument(
        "--json", dest="json_path", type=Path,
        help="Optional path to write the full analysis as JSON."
    )
    args = parser.parse_args()

    report = analyse_session(args.session_zip)
    print_report(report)

    if args.json_path:
        args.json_path.write_text(
            json.dumps(report, indent=2), encoding="utf-8"
        )
        print(f"\nJSON report written to: {args.json_path}")


if __name__ == "__main__":
    main()
