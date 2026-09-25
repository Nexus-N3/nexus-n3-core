"""Run two ankle DOTs alongside the Movesense -> CORE 2 integration."""

import argparse

from nexus_n3_tests.core.test_4f_movesense_core_hr_integration_30s import (
    Client,
    validate_movesense_core_integration,
)


class DiagnosticClient(Client):
    def handle_event(self, event):
        if event.get("type") == "sensor_diagnostics":
            print(
                "SENSOR DIAGNOSTICS:",
                event.get("payload", {}),
                flush=True,
            )

        super().handle_event(event)


DEFAULT_SUBJECTS = [
    {
        "subject_id": "subject1",
        "sensors": [
            
            {
                "local_name": "Movella DOT",
                "number_of": 2,
                "compute_algorithm": {
                    "name": "standard_loading_intensity",
                    "inputs": {
                        "gravity": 9.80665,
                    },
                },
                "locations": ["LEFT_ANKLE", "RIGHT_ANKLE"],
            },
            {
                "local_name": "Movesense",
                "number_of": 1,
                "compute_algorithm": {},
                "locations": ["CHEST"],
            },
            {
                "local_name": "Core 2",
                "number_of": 1,
                "compute_algorithm": {},
                "locations": ["CHEST"],
            },
            
      
        ],
    }
]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run a live Nexus N3 session with left/right ankle Movella DOTs, "
            "Movesense, and CORE 2."
        )
    )
    parser.add_argument("--cmd-pub-addr", default="tcp://localhost:5555")
    parser.add_argument("--evt-sub-addr", default="tcp://localhost:5556")
    parser.add_argument("--stream-seconds", type=int, default=30)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.stream_seconds <= 0:
        raise SystemExit("--stream-seconds must be a positive integer")

    if args.timeout_seconds <= args.stream_seconds:
        raise SystemExit("--timeout-seconds must be greater than --stream-seconds")

    client = DiagnosticClient(
        cmd_pub_addr=args.cmd_pub_addr,
        evt_sub_addr=args.evt_sub_addr,
        stream_seconds=args.stream_seconds,
        subjects=DEFAULT_SUBJECTS,
        session_label="movella_movesense_core_integration_30s",
    )

    ok = client.run(timeout_seconds=args.timeout_seconds)

    if not ok:
        raise SystemExit(
            client.failure_reason
            or "Movella DOT/Movesense/CORE 2 integration session failed"
        )

    if not client.official_started:
        raise SystemExit("FAILED: official stream never started")

    if not client.stream_drained_payload:
        raise SystemExit("FAILED: no stream_drained payload")

    try:
        validate_movesense_core_integration(client.stream_drained_payload)
    except Exception as exc:
        raise SystemExit(f"FAILED integration validation: {exc}") from exc

    print(
        "\nPASSED: two Movella DOTs + Movesense -> CORE 2 integration test"
    )
