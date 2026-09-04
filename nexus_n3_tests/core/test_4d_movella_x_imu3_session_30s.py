"""Run a live 30-second session with one Movella DOT and one X-IMU3."""

import argparse

from nexus_n3_tests.core.test_4c_two_sensor_session_30s import Client


DEFAULT_SUBJECTS = [
    {
        "subject_id": "subject1",
        "sensors": [
            {
                "local_name": "Movella DOT",
                "number_of": 1,
                "attributes": {"SAMPLING_RATE": 60},
                "compute_algorithm": {
                    "name": "standard_loading_intensity",
                    "inputs": {
                        "gravity": 9.80665,
                    },
                },
                "locations": ["HEAD"],
            },
            {
                "local_name": "X-IMU3",
                "number_of": 1,
                "attributes": {"SAMPLING_RATE": 100},
                "compute_algorithm": {
                    "name": "standard_loading_intensity",
                    "inputs": {
                        "gravity": 9.80665,
                    },
                },
                "locations": ["CHEST"],
            },
        ],
    }
]


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run a live Nexus N3 Core session with one Movella DOT and one "
            "100 Hz X-IMU3."
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

    client = Client(
        cmd_pub_addr=args.cmd_pub_addr,
        evt_sub_addr=args.evt_sub_addr,
        stream_seconds=args.stream_seconds,
        subjects=DEFAULT_SUBJECTS,
        session_label="movella_x_imu3_session_30s",
    )
    ok = client.run(timeout_seconds=args.timeout_seconds)
    if not ok:
        raise SystemExit(
            client.failure_reason or "Movella DOT and X-IMU3 session test failed"
        )
