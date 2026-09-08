"""Run a distributed 30-second session across the master and one worker.

This is a live hardware test. Start Core in master mode and ensure exactly one
worker is registered before running it.

The master's least-loaded assignment policy should put the first subject on
the master and the second on the worker when both nodes advertise the required
Movella DOT and Standard Loading Intensity capabilities.

For this test the subjects are prepared sequentially:

    1. Discover sensors for the master subject.
    2. Connect sensors for the master subject.
    3. Discover sensors for the worker subject.
    4. Connect sensors for the worker subject.
    5. Start streaming for all subjects.

This verifies the subject-specific distributed discovery/connection lifecycle
before testing the higher-level discover-all/connect-all orchestration.
"""

import argparse
import threading

from nexus_n3.gateway.messaging import message_types as mt
from nexus_n3_tests.core.test_4c_two_sensor_session_30s import Client


MASTER_SUBJECT_ID = "subject_master"
WORKER_SUBJECT_ID = "subject_worker"

DEFAULT_SUBJECTS = [
    {
        "subject_id": MASTER_SUBJECT_ID,
        "sensors": [
            {
                "local_name": "Movella DOT",
                "number_of": 2,
                "compute_algorithm": {
                    "name": "standard_loading_intensity",
                    "inputs": {"gravity": 9.80665},
                },
                "locations": ["LEFT_ANKLE", "RIGHT_ANKLE"],
            }
        ],
    },
    {
        "subject_id": WORKER_SUBJECT_ID,
        "sensors": [
            {
                "local_name": "Movella DOT",
                "number_of": 2,
                "compute_algorithm": {
                    "name": "standard_loading_intensity",
                    "inputs": {"gravity": 9.80665},
                },
                "locations": ["LEFT_ANKLE", "RIGHT_ANKLE"],
            }
        ],
    },
]


class DistributedClient(Client):
    """Coordinate lifecycle events emitted independently by two nodes."""

    def __init__(self, *, worker_node_id: str, **kwargs):
        super().__init__(subjects=DEFAULT_SUBJECTS, **kwargs)

        self.worker_node_id = worker_node_id

        self.expected_subject_ids = {
            MASTER_SUBJECT_ID,
            WORKER_SUBJECT_ID,
        }

        # Deliberately process subjects one at a time.
        self.subject_sequence = [
            MASTER_SUBJECT_ID,
            WORKER_SUBJECT_ID,
        ]

        self.current_subject_index = 0

        self.expected_sensors_by_subject = {
            subject["subject_id"]: sum(
                sensor.get("number_of", 1)
                for sensor in subject.get("sensors", [])
            )
            for subject in DEFAULT_SUBJECTS
        }

        self.initialized_subject_ids = set()
        self.started_subject_ids = set()
        self.stopped_subject_ids = set()
        self.drained_node_ids = set()
        self.compute_subject_ids = set()

        self.subject_assignments = {}

        # Tuple:
        #
        #   (node_id, subject_id, physical_address)
        #
        self.discovered_instances = set()
        self.connected_instances = set()

        # Tuple:
        #
        #   (node_id, physical_address)
        #
        self.disconnected_instances = set()

        self.discovery_sent_for = set()
        self.connect_sent_for = set()

        self.init_sent = False
        self.stream_start_sent = False
        self.stop_timer_started = False
        self.disconnect_sent = False

    # ------------------------------------------------------------------
    # Payload helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _subject_ids(payload):
        subject_ids = set(payload.get("subject_ids") or [])

        subject_id = payload.get("subject_id")
        if subject_id:
            subject_ids.add(subject_id)

        for subject in payload.get("subjects") or []:
            if not isinstance(subject, dict):
                continue

            subject_id = subject.get("subject_id")

            if subject_id:
                subject_ids.add(subject_id)

        return subject_ids

    @staticmethod
    def _sensor_address(sensor):
        if isinstance(sensor, str):
            return sensor

        if isinstance(sensor, dict):
            return sensor.get("address")

        return None

    @classmethod
    def _sensor_instances(cls, node_id, payload, field_name):
        """Extract node/subject/address tuples from a sensor event."""

        instances = set()

        # Normal aggregate event structure:
        #
        # {
        #     "subjects": [
        #         {
        #             "subject_id": "...",
        #             "discovered_sensors": [...]
        #         }
        #     ]
        # }
        for subject in payload.get("subjects") or []:
            if not isinstance(subject, dict):
                continue

            subject_id = subject.get("subject_id")

            for sensor in subject.get(field_name) or []:
                address = cls._sensor_address(sensor)

                if address:
                    instances.add(
                        (
                            node_id,
                            subject_id,
                            str(address),
                        )
                    )

        # Also support a subject-specific event payload:
        #
        # {
        #     "subject_id": "...",
        #     "discovered_sensors": [...]
        # }
        subject_id = payload.get("subject_id")

        if subject_id:
            for sensor in payload.get(field_name) or []:
                address = cls._sensor_address(sensor)

                if address:
                    instances.add(
                        (
                            node_id,
                            subject_id,
                            str(address),
                        )
                    )

        return instances

    @staticmethod
    def _normalise_address(address):
        return str(address).strip().lower()

    @classmethod
    def _physical_addresses(cls, instances):
        return {
            cls._normalise_address(instance[-1])
            for instance in instances
            if instance[-1]
        }

    @classmethod
    def _addresses_for_subject(cls, instances, subject_id):
        return {
            cls._normalise_address(address)
            for _node_id, instance_subject_id, address in instances
            if instance_subject_id == subject_id and address
        }

    @classmethod
    def _sensor_owners(cls, instances):
        """Map physical sensor address to subjects/nodes that report it."""

        owners = {}

        for node_id, subject_id, address in instances:
            if not address:
                continue

            normalized = cls._normalise_address(address)

            owners.setdefault(normalized, set()).add(
                (node_id, subject_id)
            )

        return owners

    def _validate_unique_sensor_ownership(self, instances, stage):
        """Ensure one physical sensor is not allocated to two subjects."""

        owners = self._sensor_owners(instances)

        conflicts = {
            address: sorted(sensor_owners)
            for address, sensor_owners in owners.items()
            if len(sensor_owners) > 1
        }

        if not conflicts:
            return True

        details = "; ".join(
            f"{address} -> {sensor_owners}"
            for address, sensor_owners in sorted(conflicts.items())
        )

        self._fail(
            f"Physical sensor ownership conflict during {stage}: "
            f"{details}. "
            "The same physical sensor was allocated to multiple subjects."
        )

        return False

    # ------------------------------------------------------------------
    # Subject assignment
    # ------------------------------------------------------------------

    def _record_assignments(self, event, subject_ids):
        assigned_node = event.get("node_id") or "master"

        for subject_id in subject_ids:
            previous = self.subject_assignments.setdefault(
                subject_id,
                assigned_node,
            )

            if previous != assigned_node:
                self._fail(
                    f"Subject {subject_id!r} emitted events from both "
                    f"{previous!r} and {assigned_node!r}."
                )

    def _validate_assignments(self):
        expected = {
            MASTER_SUBJECT_ID: "master",
            WORKER_SUBJECT_ID: self.worker_node_id,
        }

        if self.subject_assignments != expected:
            self._fail(
                "Unexpected subject assignment: "
                f"expected {expected}, observed {self.subject_assignments}. "
                "Ensure exactly one worker is registered and both nodes have "
                "the Movella DOT and Standard Loading Intensity plugins."
            )
            return False

        print("ASSIGNMENTS:", self.subject_assignments)

        return True

    # ------------------------------------------------------------------
    # Sequential subject preparation
    # ------------------------------------------------------------------

    def _current_subject_id(self):
        if self.current_subject_index >= len(self.subject_sequence):
            return None

        return self.subject_sequence[self.current_subject_index]

    def _discover_subject(self, subject_id):
        if subject_id in self.discovery_sent_for:
            return

        self.discovery_sent_for.add(subject_id)

        assigned_node = self.subject_assignments.get(subject_id)

        print(
            f"PREPARE: discover subject={subject_id} "
            f"node={assigned_node}"
        )

        self.send_command(
            {
                "type": mt.CMD_DISCOVER_SENSORS_FOR_SUBJECTS,
                "payload": {
                    "subject_ids": [subject_id],
                },
            }
        )

    def _connect_subject(self, subject_id):
        if subject_id in self.connect_sent_for:
            return

        self.connect_sent_for.add(subject_id)

        assigned_node = self.subject_assignments.get(subject_id)

        print(
            f"PREPARE: connect subject={subject_id} "
            f"node={assigned_node}"
        )

        self.send_command(
            {
                "type": mt.CMD_CONNECT_SUBJECTS,
                "payload": {
                    "subject_ids": [subject_id],
                },
            }
        )

    def _advance_to_next_subject(self):
        self.current_subject_index += 1

        next_subject_id = self._current_subject_id()

        if next_subject_id is not None:
            print(
                f"PREPARE: advancing to subject={next_subject_id}"
            )

            self._discover_subject(next_subject_id)

            return

        # Every subject has now been discovered and connected.
        physical_addresses = self._physical_addresses(
            self.connected_instances
        )

        if len(physical_addresses) != self.expected_sensor_count:
            self._fail(
                "Unexpected physical sensor count after sequential "
                f"connection: expected {self.expected_sensor_count}, "
                f"observed {len(physical_addresses)}: "
                f"{sorted(physical_addresses)}"
            )
            return

        if not self._validate_unique_sensor_ownership(
            self.connected_instances,
            "final connection validation",
        ):
            return

        print(
            "PREPARE COMPLETE:",
            sorted(self.connected_instances),
        )

        print(
            f"CONNECTED PHYSICAL SENSORS: "
            f"{sorted(physical_addresses)}"
        )

        self.stream_start_sent = True

        self.send_command(
            {
                "type": mt.CMD_START_STREAM_FOR_ALL,
                "payload": {
                    "tag": self.session_label,
                },
            }
        )

    # ------------------------------------------------------------------
    # Event handling
    # ------------------------------------------------------------------

    def handle_event(self, event):
        evt_type = event.get("type")
        payload = event.get("payload") or {}
        node_id = event.get("node_id") or "master"

        print(f"EVENT: {evt_type} node={node_id}")

        # --------------------------------------------------------------
        # SERVER READY
        # --------------------------------------------------------------

        if evt_type == mt.EVT_SERVER_READY:
            if not self.init_sent:
                self.init_sent = True

                self.send_command(
                    {
                        "type": mt.CMD_INIT_SYSTEM,
                        "payload": {
                            "subjects": self.subjects,
                            "init_label": self.session_label,
                        },
                    }
                )

            return

        # --------------------------------------------------------------
        # SYSTEM INITIALISED
        # --------------------------------------------------------------

        if evt_type == mt.EVT_SYSTEM_INITIALIZED:
            subject_ids = self._subject_ids(payload)

            self.initialized_subject_ids.update(subject_ids)

            self._record_assignments(
                event,
                subject_ids,
            )

            if self.failed:
                return

            if (
                self.initialized_subject_ids >= self.expected_subject_ids
                and not self.discovery_sent_for
            ):
                if not self._validate_assignments():
                    return

                first_subject_id = self._current_subject_id()

                if first_subject_id is None:
                    self._fail(
                        "No subjects available for distributed test."
                    )
                    return

                self._discover_subject(first_subject_id)

            return

        # --------------------------------------------------------------
        # SENSORS DISCOVERED
        # --------------------------------------------------------------

        if evt_type in {
            mt.EVT_SENSORS_DISCOVERED,
            mt.EVT_SENSORS_DISCOVERED_FOR_SUBJECT,
        }:
            discovered = self._sensor_instances(
                node_id,
                payload,
                "discovered_sensors",
            )

            if not discovered:
                print(
                    "DISCOVERED event contained no sensor instances:",
                    payload,
                )
                return

            self.discovered_instances.update(discovered)

            print(
                "DISCOVERED:",
                sorted(discovered),
            )

            current_subject_id = self._current_subject_id()

            if current_subject_id is None:
                return

            current_addresses = self._addresses_for_subject(
                self.discovered_instances,
                current_subject_id,
            )

            expected_count = self.expected_sensors_by_subject[
                current_subject_id
            ]

            print(
                f"DISCOVERED SUBJECT: {current_subject_id} "
                f"{sorted(current_addresses)} "
                f"({len(current_addresses)}/{expected_count})"
            )

            # Do not proceed until the currently prepared subject has
            # discovered all of its required physical sensors.
            if len(current_addresses) < expected_count:
                return

            # Because the previous subject is already connected before the
            # next subject is discovered, discovering an already allocated
            # physical address here is a genuine conflict.
            if not self._validate_unique_sensor_ownership(
                self.discovered_instances,
                "sequential discovery",
            ):
                return

            self._connect_subject(current_subject_id)

            return

        # --------------------------------------------------------------
        # SENSOR CONNECTED
        # --------------------------------------------------------------

        if evt_type == mt.EVT_SENSOR_CONNECTED:
            connected = self._sensor_instances(
                node_id,
                payload,
                "connected_sensors",
            )

            if not connected:
                print(
                    "CONNECTED event contained no sensor instances:",
                    payload,
                )
                return

            self.connected_instances.update(connected)

            print(
                "CONNECTED:",
                sorted(connected),
            )

            if not self._validate_unique_sensor_ownership(
                self.connected_instances,
                "connection",
            ):
                return

            current_subject_id = self._current_subject_id()

            if current_subject_id is None:
                return

            current_addresses = self._addresses_for_subject(
                self.connected_instances,
                current_subject_id,
            )

            expected_count = self.expected_sensors_by_subject[
                current_subject_id
            ]

            print(
                f"CONNECTED SUBJECT: {current_subject_id} "
                f"{sorted(current_addresses)} "
                f"({len(current_addresses)}/{expected_count})"
            )

            if len(current_addresses) < expected_count:
                return

            print(
                f"SUBJECT READY: {current_subject_id} "
                f"node={self.subject_assignments.get(current_subject_id)}"
            )

            # Only after this subject is fully connected do we allow the
            # next subject/node to begin discovery.
            self._advance_to_next_subject()

            return

        # --------------------------------------------------------------
        # STREAM STARTED
        # --------------------------------------------------------------

        if evt_type == mt.EVT_STREAM_STARTED:
            self.started_subject_ids.update(
                self._subject_ids(payload)
            )

            print(
                "STREAM STARTED:",
                sorted(self.started_subject_ids),
            )

            if (
                self.started_subject_ids >= self.expected_subject_ids
                and not self.stop_timer_started
            ):
                self.stop_timer_started = True

                threading.Thread(
                    target=self._stop_stream_after_delay,
                    daemon=True,
                ).start()

            return

        # --------------------------------------------------------------
        # COMPUTE RESULT
        # --------------------------------------------------------------

        if evt_type == mt.EVT_COMPUTE_RESULT:
            if (
                payload.get("algorithm_name")
                == "standard_loading_intensity"
            ):
                subject_id = payload.get("subject_id")

                if subject_id:
                    self.compute_subject_ids.add(subject_id)

                print(
                    "COMPUTE RESULT:",
                    subject_id,
                    payload.get("algorithm_name"),
                )

            return

        # --------------------------------------------------------------
        # STREAM STOPPED
        # --------------------------------------------------------------

        if evt_type == mt.EVT_STREAM_STOPPED:
            self.stopped_subject_ids.update(
                self._subject_ids(payload)
            )

            print(
                "STREAM STOPPED:",
                sorted(self.stopped_subject_ids),
            )

            return

        # --------------------------------------------------------------
        # STREAM DRAINED
        # --------------------------------------------------------------

        if evt_type == mt.EVT_STREAM_DRAINED:
            if payload.get("status", "ok") != "ok":
                self._fail(
                    f"Stream drain failed on {node_id}: "
                    f"{payload.get('reason') or payload}"
                )
                return

            self.drained_node_ids.add(node_id)

            expected_nodes = {
                "master",
                self.worker_node_id,
            }

            print(
                "STREAM DRAINED:",
                sorted(self.drained_node_ids),
            )

            if (
                self.drained_node_ids >= expected_nodes
                and not self.disconnect_sent
            ):
                self.disconnect_sent = True

                self.send_command(
                    {
                        "type": mt.CMD_DISCONNECT_ALL,
                    }
                )

            return

        # --------------------------------------------------------------
        # SENSOR DISCONNECTED
        # --------------------------------------------------------------

        if evt_type == mt.EVT_SENSOR_DISCONNECTED:
            disconnected = (
                payload.get("disconnected_sensors") or []
            )

            if isinstance(disconnected, dict):
                disconnected = [disconnected]

            for sensor in disconnected:
                address = self._sensor_address(sensor)

                if address:
                    self.disconnected_instances.add(
                        (
                            node_id,
                            str(address),
                        )
                    )

            physical_addresses = self._physical_addresses(
                self.disconnected_instances
            )

            print(
                "DISCONNECTED:",
                sorted(self.disconnected_instances),
                f"physical={len(physical_addresses)}/"
                f"{self.expected_sensor_count}",
            )

            if (
                self.disconnect_sent
                and len(physical_addresses)
                >= self.expected_sensor_count
            ):
                missing_compute = (
                    self.expected_subject_ids
                    - self.compute_subject_ids
                )

                if missing_compute:
                    self._fail(
                        "No Standard Loading Intensity result was "
                        "observed for: "
                        f"{sorted(missing_compute)}"
                    )
                else:
                    self._done.set()

            return

        # --------------------------------------------------------------
        # ERROR
        # --------------------------------------------------------------

        if evt_type == mt.EVT_ERROR:
            self._fail(
                f"Gateway error from {node_id}: {payload}"
            )


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Run two two-Movella-DOT subjects for 30 seconds, "
            "assigning one subject to the master and one to a worker."
        )
    )

    parser.add_argument(
        "--cmd-pub-addr",
        default="tcp://localhost:5555",
    )

    parser.add_argument(
        "--evt-sub-addr",
        default="tcp://localhost:5556",
    )

    parser.add_argument(
        "--stream-seconds",
        type=int,
        default=30,
    )

    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=240,
    )

    parser.add_argument(
        "--worker-node-id",
        default="nexus-n3-worker-01",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.stream_seconds <= 0:
        raise SystemExit(
            "--stream-seconds must be a positive integer"
        )

    if args.timeout_seconds <= args.stream_seconds:
        raise SystemExit(
            "--timeout-seconds must be greater than "
            "--stream-seconds"
        )

    client = DistributedClient(
        cmd_pub_addr=args.cmd_pub_addr,
        evt_sub_addr=args.evt_sub_addr,
        stream_seconds=args.stream_seconds,
        worker_node_id=args.worker_node_id,
        session_label="two_subject_distributed_session_30s",
    )

    ok = client.run(
        timeout_seconds=args.timeout_seconds,
    )

    if not ok:
        raise SystemExit(
            client.failure_reason
            or "Distributed session test failed"
        )
