"""Ordered Connect All, shared by the master and standalone Core.

Only discovery/connection results advance this workflow. Identification retains
its existing invocation semantics and is deliberately not a completion gate.
"""

from collections import Counter
from contextvars import ContextVar
from copy import deepcopy
import threading
import uuid

from nexus_n3.gateway.messaging import message_types as mt

# Confine result capture to the queued command, not callbacks on other threads.
connection_step_capture = ContextVar("connection_step_capture", default=None)


def normalized(address):
    return str(address or "").strip().upper()


class SubjectConnectionSequence:
    def __init__(self, emit, dispatch):
        self.emit = emit
        self.dispatch = dispatch
        self.lock = threading.RLock()
        self.subjects = []
        self.selected = {}
        self.completed = set()
        self.discovered = set()
        self.active = False
        self.pending = None
        self.timer = None
        self.correlation_id = None
        self.sequence_id = None
        self.index = 0

    def configure(self, subjects):
        with self.lock:
            self._cancel_timer()
            self.subjects = deepcopy(subjects)
            self.selected.clear()
            self.completed.clear()
            self.discovered.clear()
            self.active = False
            self.pending = None
            self.sequence_id = None

    @property
    def busy(self):
        with self.lock:
            return self.active or self.pending is not None

    def exclusions(self, subject_ids):
        with self.lock:
            return sorted({address for sid, addresses in self.selected.items()
                           if sid not in subject_ids for address in addresses})

    def _cancel_timer(self):
        if self.timer:
            self.timer.cancel()
            self.timer = None

    def _progress(self, phase, subject=None, **extra):
        subject = subject or {}
        self.emit({"type": mt.EVT_SUBJECT_CONNECTION_PROGRESS, "payload": {
            "correlation_id": self.correlation_id,
            "connection_sequence_id": self.sequence_id,
            "subject_id": subject.get("subject_id"),
            "node_id": subject.get("node_id"),
            "phase": phase,
            "operation_correlation_id": self.pending["token"] if self.pending else None,
            "completed_subject_ids": [s["subject_id"] for s in self.subjects
                                      if s["subject_id"] in self.completed],
            **extra,
        }})

    def start(self, correlation_id):
        with self.lock:
            if self.busy:
                self.emit({"type": mt.EVT_SUBJECT_CONNECTION_PROGRESS, "payload": {
                    "correlation_id": correlation_id, "phase": "failed",
                    "subject_id": self.pending["subject"]["subject_id"] if self.pending else None,
                    "node_id": self.pending["subject"]["node_id"] if self.pending else None,
                    "operation": "connect_all", "message": "Connect All is still in progress; wait for completion or Reset",
                }})
                return
            if not self.subjects:
                raise ValueError("No configured subjects for Connect All")
            self.correlation_id = correlation_id or uuid.uuid4().hex
            self.sequence_id = uuid.uuid4().hex
            self.active = True
            self.index = 0
            self._next_subject()

    def _next_subject(self):
        while self.index < len(self.subjects):
            subject = self.subjects[self.index]
            sid = subject["subject_id"]
            if sid in self.completed:
                self._progress("subject_connected", subject, skipped=True)
                self.index += 1
                continue
            # A connection retry retains its selected identities; do not rescan
            # connected (non-advertising) sensors or assign another pair.
            self._step(subject, "connecting" if sid in self.discovered else "discovering")
            return
        self.active = False
        self._progress("completed", self.subjects[-1])

    def _step(self, subject, operation):
        token = uuid.uuid4().hex
        self.pending = {"token": token, "subject": subject, "operation": operation}
        self._progress(operation, subject)
        sid = subject["subject_id"]
        payload = {
            "correlation_id": token,
            "connection_sequence_id": self.sequence_id,
            "subject_ids": [sid],
        }
        if operation == "discovering":
            payload["excluded_addresses"] = sorted({
                address for owner, addresses in self.selected.items()
                if owner != sid for address in addresses
            })
            command = mt.CMD_DISCOVER_SENSORS_FOR_SUBJECTS
        else:
            payload["selected_addresses"] = list(self.selected[sid])
            command = mt.CMD_CONNECT_SUBJECTS
        # Allow existing scans, sequential connections and setup their normal
        # budgets. A timeout stops progression, but leaves the outstanding step
        # blocked until its terminal reply or an authoritative Reset.
        timeout = 30 + 45 * len(subject["locations"])
        self.timer = threading.Timer(timeout, self._expired, args=(token,))
        self.timer.daemon = True
        self.timer.start()
        try:
            self.dispatch(subject["node_id"], command, payload)
        except Exception as exc:
            self._fail(str(exc))
            self.pending = None

    def _expired(self, token):
        with self.lock:
            if self.pending and self.pending["token"] == token and self.active:
                self._fail("Timed out waiting for operation completion; wait for its result or Reset before retrying")

    def _fail(self, message, subject=None, operation=None):
        self._cancel_timer()
        pending = self.pending or {}
        subject = subject or pending.get("subject", {})
        self.active = False
        self._progress("failed", subject, operation=operation or pending.get("operation"),
                       message=message,
                       selected_addresses=self.selected.get(subject.get("subject_id"), []))

    def on_event(self, event):
        with self.lock:
            kind = event.get("type")
            payload = event.get("payload")
            if not isinstance(payload, dict):
                return
            if kind == mt.EVT_SENSOR_CLEANUP_STARTED:
                if self.active:
                    self._fail("Connect All interrupted by sensor cleanup")
                return
            if kind == mt.EVT_SENSOR_CLEANUP_COMPLETE:
                # Reset retires configuration even when physical recovery is
                # incomplete; its existing Core readiness flag is authoritative.
                if payload.get("recover_session") and payload.get("core_ready_for_configuration") is True:
                    node = payload.get("node_id", event.get("node_id"))
                    for subject in self.subjects:
                        if subject["node_id"] == node:
                            self.selected.pop(subject["subject_id"], None)
                            self.completed.discard(subject["subject_id"])
                            self.discovered.discard(subject["subject_id"])
                    if self.pending and self.pending["subject"]["node_id"] == node:
                        if self.active:
                            self._fail("Connect All interrupted by Reset")
                        self.pending = None
                return
            if kind == mt.EVT_SENSOR_DISCONNECTED:
                lost = {normalized(a) for a in payload.get("disconnected_sensors", [])}
                for sid, addresses in self.selected.items():
                    if lost.intersection(addresses):
                        was_complete = sid in self.completed
                        self.completed.discard(sid)
                        if self.active and was_complete:
                            subject = next(s for s in self.subjects if s["subject_id"] == sid)
                            self._fail("Previously connected subject lost a sensor", subject, "connecting")
                return
            if kind == mt.EVT_SENSORS_DISCOVERED and not payload.get("connection_sequence_id"):
                for result in payload.get("subjects", []):
                    sid = result.get("subject_id")
                    if any(subject["subject_id"] == sid for subject in self.subjects):
                        self.completed.discard(sid)
                        self.discovered.discard(sid)
                        self.selected[sid] = [normalized(a) for a in result.get("discovered_sensors", [])]
            pending = self.pending
            if (not pending or payload.get("correlation_id") != pending["token"]
                    or payload.get("connection_sequence_id") != self.sequence_id):
                return
            subject = pending["subject"]
            if payload.get("node_id", event.get("node_id")) != subject["node_id"]:
                return
            if kind not in {mt.EVT_SENSORS_DISCOVERED, mt.EVT_SENSOR_CONNECTED, mt.EVT_ERROR}:
                return
            expected = (mt.EVT_SENSORS_DISCOVERED if pending["operation"] == "discovering"
                        else mt.EVT_SENSOR_CONNECTED)
            if kind != mt.EVT_ERROR and kind != expected:
                return
            self._cancel_timer()
            sid = subject["subject_id"]
            selected = [normalized(a) for a in payload.get("selected_addresses", [])]
            # Keep even partial selections for exclusion/reset diagnostics.
            if selected and pending["operation"] == "discovering":
                self.selected[sid] = selected
            if kind == mt.EVT_ERROR:
                if self.active:
                    self._fail(payload.get("message", "Subject operation failed"))
                self.pending = None
                return
            results = [s for s in payload.get("subjects", []) if s.get("subject_id") == sid]
            valid = len(results) == 1 and payload.get("operation_complete") is True
            if pending["operation"] == "discovering":
                addresses = [normalized(a) for a in results[0].get("discovered_sensors", [])] if results else []
                others = {a for owner, values in self.selected.items() if owner != sid for a in values}
                valid = (valid and len(addresses) == len(subject["locations"])
                         and all(addresses) and len(set(addresses)) == len(addresses)
                         and not others.intersection(addresses))
                if not valid:
                    if self.active:
                        self._fail("Incomplete discovery or duplicate sensor addresses")
                    self.pending = None
                    return
                self.selected[sid] = addresses
                self.discovered.add(sid)
                self.pending = None
                if self.active:
                    self._step(subject, "connecting")
            else:
                sensors = results[0].get("connected_sensors", []) if results else []
                addresses = [normalized(s.get("address")) for s in sensors]
                valid = (valid and len(addresses) == len(subject["locations"])
                         and len(set(addresses)) == len(addresses)
                         and set(addresses) == set(self.selected.get(sid, []))
                         and Counter((normalized(s.get("address")), s.get("location")) for s in sensors)
                         == Counter(zip(self.selected.get(sid, []), subject["locations"]))
                         and all(s.get("status") == "CONNECTED" and s.get("setup_complete") is True for s in sensors))
                if not valid:
                    if self.active:
                        self._fail("Incomplete connection or sensor setup")
                    self.pending = None
                    return
                self.completed.add(sid)
                self.pending = None
                if not self.active:
                    return  # A late terminal reply never advances a sequence.
                self._progress("subject_connected", subject)
                # No device acknowledgment, identifying phase or progression gate.
                try:
                    self.dispatch(subject["node_id"], mt.CMD_IDENTIFY_SENSOR, {
                        "subject_id": sid, "connection_sequence_id": self.sequence_id,
                        "correlation_id": uuid.uuid4().hex,
                        "sequence_correlation_id": self.correlation_id,
                    })
                except Exception as exc:
                    self.emit({"type": mt.EVT_ERROR, "payload": {
                        "correlation_id": self.correlation_id, "subject_id": sid,
                        "node_id": subject["node_id"], "operation": "identify",
                        "severity": "warning", "message": str(exc),
                    }})
                self.index += 1
                self._next_subject()
