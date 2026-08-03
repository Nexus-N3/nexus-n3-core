"""Low-overhead pipeline diagnostics for local session review."""

from __future__ import annotations

import json
import time
from collections import defaultdict
from pathlib import Path
from queue import Queue
from threading import Lock, Thread


class PipelineDiagnostics:
    """Aggregates per-sensor counters and writes periodic session snapshots."""

    def __init__(self, snapshot_interval_seconds: float = 5.0):
        self._snapshot_interval_seconds = snapshot_interval_seconds
        self._enabled = False
        self._lock = Lock()
        self._records = Queue()
        self._output_path: Path | None = None
        self._session_key: str | None = None
        self._session_meta: dict = {}
        self._sensor_meta: dict[str, dict] = {}
        self._counters = defaultdict(lambda: defaultdict(int))
        self._queue_depth = 0
        self._stream_start_command_monotonic: dict[str, float] = {}
        self._first_ble_notify_seen: set[str] = set()
        self._writer_thread = Thread(target=self._writer_loop, name="nexus-n3-pipeline-diag-writer", daemon=True)
        self._writer_thread.start()
        self._snapshot_thread = Thread(
            target=self._snapshot_loop,
            name="nexus-n3-pipeline-diag-snapshot",
            daemon=True,
        )
        self._snapshot_thread.start()

    def set_enabled(self, enabled: bool) -> None:
        with self._lock:
            self._enabled = bool(enabled)

    def is_enabled(self) -> bool:
        with self._lock:
            return self._enabled

    def start_session(
        self,
        session_dir: str | Path,
        *,
        site: str | None,
        session_label: str | None,
        session_timestamp: str | None,
    ) -> None:
        if not self.is_enabled():
            return
        session_path = Path(session_dir).resolve()
        output_path = session_path / "diagnostics" / "pipeline_debug.ndjson"
        session_key = str(output_path)
        with self._lock:
            if self._session_key == session_key:
                return
            self._output_path = output_path
            self._session_key = session_key
            self._session_meta = {
                "site": site,
                "session_label": session_label,
                "session_timestamp": session_timestamp,
                "session_dir": str(session_path),
            }
            self._sensor_meta = {}
            self._counters = defaultdict(lambda: defaultdict(int))
            self._queue_depth = 0
            self._stream_start_command_monotonic = {}
            self._first_ble_notify_seen = set()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self.record_event("session_start", **self._session_meta)

    def register_sensor(
        self,
        *,
        address: str | None,
        subject_id: str | None,
        location: str | None,
        tag: str | None,
    ) -> None:
        if not self.is_enabled():
            return
        if not address:
            return
        with self._lock:
            meta = self._sensor_meta.setdefault(address, {})
            if subject_id is not None:
                meta["subject_id"] = subject_id
            if location is not None:
                meta["location"] = location
            if tag is not None:
                meta["tag"] = tag

    def increment(self, address: str | None, counter: str, amount: int = 1, **metadata) -> None:
        if not self.is_enabled():
            return
        if not address:
            return
        with self._lock:
            meta = self._sensor_meta.setdefault(address, {})
            for key, value in metadata.items():
                if value is not None:
                    meta[key] = value
            self._counters[address][counter] += amount

    def set_queue_depth(self, depth: int) -> None:
        if not self.is_enabled():
            return
        with self._lock:
            self._queue_depth = max(int(depth), 0)

    def mark_stream_start_command(self, address: str | None, **metadata) -> None:
        if not self.is_enabled():
            return
        if not address:
            return
        monotonic_now = time.monotonic()
        with self._lock:
            self._stream_start_command_monotonic[address] = monotonic_now
            meta = self._sensor_meta.setdefault(address, {})
            for key, value in metadata.items():
                if value is not None:
                    meta[key] = value
        self.record_event(
            "stream_start_command",
            address=address,
            monotonic_seconds=round(monotonic_now, 6),
            **metadata,
        )

    def mark_first_ble_notify(self, address: str | None, **metadata) -> None:
        if not self.is_enabled():
            return
        if not address:
            return
        monotonic_now = time.monotonic()
        with self._lock:
            if address in self._first_ble_notify_seen:
                return
            self._first_ble_notify_seen.add(address)
            start_mono = self._stream_start_command_monotonic.get(address)
            meta = self._sensor_meta.setdefault(address, {})
            for key, value in metadata.items():
                if value is not None:
                    meta[key] = value
        latency_ms = None
        if start_mono is not None:
            latency_ms = round((monotonic_now - start_mono) * 1000.0, 3)
        self.record_event(
            "first_ble_notify",
            address=address,
            first_ble_notify_monotonic_seconds=round(monotonic_now, 6),
            first_ble_notify_latency_ms=latency_ms,
            **metadata,
        )

    def record_event(self, event_type: str, **payload) -> None:
        if not self.is_enabled():
            return
        with self._lock:
            output_path = self._output_path
            session_meta = dict(self._session_meta)
            if not output_path:
                return
            record = {
                "type": event_type,
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
                **session_meta,
                **payload,
            }
            self._records.put((output_path, record))

    def flush(self) -> None:
        if not self.is_enabled():
            return
        self._records.join()

    def finish_session(self) -> None:
        """Detach the current output before archiving and drain queued writes."""
        if not self.is_enabled():
            return
        with self._lock:
            self._output_path = None
            self._session_key = None
            self._session_meta = {}
            self._sensor_meta = {}
            self._counters = defaultdict(lambda: defaultdict(int))
            self._queue_depth = 0
            self._stream_start_command_monotonic = {}
            self._first_ble_notify_seen = set()
        self._records.join()

    def _snapshot_loop(self) -> None:
        while True:
            time.sleep(self._snapshot_interval_seconds)
            self._enqueue_snapshots()

    def _enqueue_snapshots(self) -> None:
        if not self.is_enabled():
            return
        with self._lock:
            output_path = self._output_path
            if not output_path:
                return
            session_meta = dict(self._session_meta)
            queue_depth = self._queue_depth
            sensor_meta = {address: dict(meta) for address, meta in self._sensor_meta.items()}
            counters = {
                address: dict(counter_map)
                for address, counter_map in self._counters.items()
            }
            for address, counter_map in counters.items():
                meta = sensor_meta.get(address, {})
                record = {
                    "type": "snapshot",
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    **session_meta,
                    "address": address,
                    **meta,
                    **counter_map,
                    "raw_queue_depth": queue_depth,
                }
                self._records.put((output_path, record))

    def _writer_loop(self) -> None:
        while True:
            output_path, record = self._records.get()
            try:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                with output_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(record, sort_keys=True) + "\n")
            finally:
                self._records.task_done()


pipeline_diagnostics = PipelineDiagnostics()
