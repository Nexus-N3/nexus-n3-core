"""File storage management for raw and computed sensor data."""

import base64
import csv
import json
import re
from queue import Queue
from copy import deepcopy
from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from threading import Lock, Thread
from types import SimpleNamespace
from typing import Any, List

from nexus_n3.logger.logger import get_module_logger
from nexus_n3.file_manager.session_archive import archive_session_directory, build_session_archive_name
from nexus_n3.core.pipeline_diagnostics import pipeline_diagnostics

class FileManager:
    """
    Manages file storage and streaming for Nexus N3 sensor data.

    Responsibilities:
        - Prepare CSV files for each sensor per subject session.
        - Thread-safe writing of sensor sample blocks.
        - Support multiple sensor types (IMU, ECG, etc.).
        - Organize files by subject and session timestamp.

    Attributes:
        base_dir (Path): Base output directory for all subjects.
        locks (dict[Path, Lock]): File locks for thread-safe writes.
        logger: Logger instance for logging file operations.
    """

    def __init__(self, site, base_dir="nexus_n3_outputs"):
        """
        Initialize the FileManager.

        Args:
            base_dir (str | Path, optional): Base directory for CSV outputs. Defaults to "nexus_n3_outputs".
        """
        self.site = site
        self.base_root = Path(base_dir)
        self.session_label = None
        self.session_name = None
        self.base_dir = self._resolve_base_dir()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.locks = {}  # path -> Lock
        self._subject_algorithm_intermediate_paths = {}
        self._subject_algorithm_consolidated_paths = {}
        self._raw_write_queue = Queue()
        self._raw_write_failures = {}
        self._raw_write_failures_lock = Lock()
        self._session_diagnostics_lock = Lock()
        self._session_diagnostics_state = {}
        self.logger = get_module_logger("File Manager")
        self._raw_writer_thread = Thread(
            target=self._writer_loop,
            name="nexus-n3-raw-writer",
            daemon=True,
        )
        self._raw_writer_thread.start()
        self.logger.info("file manger intialised")

    # -------------------------
    # Private Methods
    # -------------------------

    def _resolve_base_dir(self) -> Path:
        base = self.base_root / self.site
        if self.session_label:
            base = base / self.session_label
        return base

    def _sanitize_label(self, label: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", label.strip())
        return cleaned or "session"

    def _sanitize_tag(self, tag: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", tag.strip())
        return cleaned or "untagged"

    def _sanitize_component(self, value: str | None, fallback: str = "na") -> str:
        raw = str(value or "").strip()
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", raw)
        return cleaned or fallback

    def _session_dir(self, session_index: str | None) -> Path | None:
        if not session_index:
            return None
        return self.base_dir / f"session_{session_index}"

    def _merge_summary(self, target: dict, updates: dict) -> None:
        for key, value in updates.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                self._merge_summary(target[key], value)
            else:
                target[key] = value

    def _write_session_diagnostics_summary_locked(self) -> None:
        session_dir = self._session_diagnostics_state.get("session_dir")
        summary_path = self._session_diagnostics_state.get("summary_path")
        summary = self._session_diagnostics_state.get("summary")
        if not summary_path or summary is None:
            return
        if session_dir and not session_dir.exists() and not summary_path.exists():
            return
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    def _build_computed_filename(
        self,
        *,
        algorithm: str | None,
        stage: str,
        activity: str,
        timestamp: str,
        location: str | None = None,
        address: str | None = None,
    ) -> str:
        # Keep a consistent naming contract while omitting sensor-only fields
        # for algorithm-level stages (intermediate/consolidated).
        parts = [
            f"algorithm_{self._sanitize_component(algorithm, 'unknown')}",
        ]
        if location is not None:
            parts.append(f"location_{self._sanitize_component(location)}")
        if address is not None:
            parts.append(f"address_{self._sanitize_component(address)}")
        parts.extend(
            [
                f"stage_{self._sanitize_component(stage)}",
                f"activity_{self._sanitize_component(activity)}",
                f"timestamp_{self._sanitize_component(timestamp)}",
            ]
        )
        return "__".join(parts) + ".ndjson"

    def _sample_to_row(self, sample: Any, headers: list[str]):
        """
        Convert a sensor sample into a CSV row.

        Args:
            sample: Sample object to convert.
            headers: CSV header order.

        Returns:
            list: Row values to write to CSV.
        """
        mapping = self._sample_to_mapping(sample)
        return [self._serialize_csv_value(mapping.get(header)) for header in headers]

    def _sample_to_mapping(self, sample: Any) -> dict[str, Any]:
        if isinstance(sample, dict):
            return {str(key): self._normalize_sample_value(value) for key, value in sample.items()}

        to_dict = getattr(sample, "to_dict", None)
        if callable(to_dict):
            payload = to_dict()
            if isinstance(payload, dict):
                return {str(key): self._normalize_sample_value(value) for key, value in payload.items()}

        if is_dataclass(sample):
            return {
                field.name: self._normalize_sample_value(getattr(sample, field.name))
                for field in fields(sample)
            }

        if hasattr(sample, "__dict__"):
            payload = {
                key: value
                for key, value in vars(sample).items()
                if not str(key).startswith("_")
            }
            sample_type = getattr(sample, "sample_type", None)
            if sample_type is not None and "sample_type" not in payload:
                payload["sample_type"] = sample_type
            return {str(key): self._normalize_sample_value(value) for key, value in payload.items()}

        return {"value": self._normalize_sample_value(sample)}

    def _normalize_sample_value(self, value: Any):
        if isinstance(value, Enum):
            return value.name
        if isinstance(value, (bytes, bytearray, memoryview)):
            return base64.b64encode(bytes(value)).decode("ascii")
        if is_dataclass(value):
            return {
                field.name: self._normalize_sample_value(getattr(value, field.name))
                for field in fields(value)
            }
        if isinstance(value, SimpleNamespace):
            return {
                str(key): self._normalize_sample_value(item)
                for key, item in vars(value).items()
                if not str(key).startswith("_")
            }
        if isinstance(value, dict):
            return {str(key): self._normalize_sample_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._normalize_sample_value(item) for item in value]
        if hasattr(value, "__dict__") and not isinstance(value, (str, int, float, bool)):
            return {
                str(key): self._normalize_sample_value(item)
                for key, item in vars(value).items()
                if not str(key).startswith("_")
            }
        return value

    def _derive_headers(self, sample_mappings: list[dict[str, Any]]) -> list[str]:
        headers: list[str] = []
        for mapping in sample_mappings:
            for key in mapping.keys():
                if key not in headers:
                    headers.append(key)
        return headers

    def _ensure_raw_headers(self, entry, path: Path, sample_mappings: list[dict[str, Any]]) -> list[str]:
        headers = list(entry["meta"].get("raw_headers") or [])
        derived_headers = self._derive_headers(sample_mappings)
        if not headers:
            headers = derived_headers
            with open(path, "w", newline="") as f:
                csv.writer(f).writerow(headers)
            entry["meta"]["raw_headers"] = list(headers)
            return headers

        missing = [header for header in derived_headers if header not in headers]
        if not missing:
            return headers

        expanded_headers = headers + missing
        existing_rows: list[dict[str, Any]] = []
        if path.exists() and path.stat().st_size > 0:
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames:
                    existing_rows = list(reader)

        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=expanded_headers)
            writer.writeheader()
            for row in existing_rows:
                writer.writerow({header: row.get(header, "") for header in expanded_headers})

        entry["meta"]["raw_headers"] = list(expanded_headers)
        return expanded_headers

    def _serialize_csv_value(self, value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=self._json_serializer, sort_keys=True)
        return value

    def _record_raw_write_failure(self, entry, path: Path, exc: Exception):
        subject_id = entry.get("meta", {}).get("subject_id") or "unknown_subject"
        sensor = entry.get("sensor")
        address = getattr(sensor, "address", None)
        location = entry.get("meta", {}).get("location")
        failure = {
            "subject_id": subject_id,
            "location": location,
            "address": address,
            "path": str(path),
            "error": f"{type(exc).__name__}: {exc}",
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        }
        with self._raw_write_failures_lock:
            self._raw_write_failures.setdefault(subject_id, []).append(failure)
        self.logger.error(
            "raw write failed for subject=%s location=%s address=%s path=%s error=%s",
            subject_id,
            location,
            address,
            path,
            failure["error"],
        )
        pipeline_diagnostics.record_event(
            "writer_error",
            subject_id=subject_id,
            address=address,
            location=location,
            path=str(path),
            error=failure["error"],
        )

    def _writer_loop(self):
        while True:
            entry, samples = self._raw_write_queue.get()
            try:
                self.write_block(entry, samples)
                address = getattr(entry.get("sensor"), "address", None)
                pipeline_diagnostics.increment(address, "raw_blocks_written", 1)
                pipeline_diagnostics.increment(address, "raw_samples_written", len(samples))
            except Exception as exc:
                path = entry.get("meta", {}).get("file_path")
                self._record_raw_write_failure(entry, path, exc)
            finally:
                pipeline_diagnostics.set_queue_depth(self._raw_write_queue.qsize())
                self._raw_write_queue.task_done()

    # -------------------------
    # Public Methods
    # -------------------------

    def set_base_path(self, path: str | None):
        """
        Set the base output directory.

        Args:
            path: Base directory path or None to use the default.
        """
        print("Setting base path to:", path)
        self.base_root = Path(path) if path else Path("nexus_n3_outputs")
        self.base_dir = self._resolve_base_dir()
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def clear_raw_write_failures(self, subject_ids: list[str]):
        """Clear stored raw write failures for the provided subjects."""
        with self._raw_write_failures_lock:
            for subject_id in subject_ids:
                self._raw_write_failures.pop(subject_id, None)

    def set_session_label(self, label: str | None):
        """
        Set the session label used as the top-level directory under the site.

        Args:
            label: Label string or None to disable label-based grouping.
        """
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        if label:
            safe_label = self._sanitize_label(label)
            self.session_name = safe_label
            self.session_label = f"{safe_label}_{ts}"
        else:
            self.session_name = "sys_session"
            self.session_label = f"sys_session_{ts}"
        self.base_dir = self._resolve_base_dir()
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def start_stream(self, subject, session_index, tag: str | None = None):
        """
        Prepare output files for a subject session.

        Args:
            subject: Subject instance.
            session_index: Session identifier used in path layout.
            tag: Optional tag to group output under a named activity directory.
        """
        self.logger.info("Preparing CSV files for streaming")
        self.clear_raw_write_failures([subject.subject_id])
        self.base_dir = self._resolve_base_dir()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        session_ts = str(session_index)
        session_dir = self.base_dir / f"session_{session_index}"
        pipeline_diagnostics.start_session(
            session_dir,
            site=self.site,
            session_label=self.session_label,
            session_timestamp=session_ts,
        )
        subject.session_timestamp = session_ts
        safe_tag = self._sanitize_tag(tag) if tag else "sys"
        tag_folder = f"{safe_tag}_{session_ts}"
        unique_algorithms = set()
        for sensor_entry in subject.sensors:
            compute_algo = sensor_entry.get("meta", {}).get("compute_algorithm", {}) or {}
            algo_name = compute_algo.get("name") or "unknown"
            unique_algorithms.add(str(algo_name))

        for algorithm_name in sorted(unique_algorithms):
            intermediate_t_path = (
                self.base_dir
                / f"session_{session_index}"
                / subject.subject_id
                / tag_folder
                / "computed"
                / "intermediate"
                / self._build_computed_filename(
                    algorithm=algorithm_name,
                    stage="intermediate_time",
                    activity=safe_tag,
                    timestamp=session_ts,
                )
            )
            intermediate_t_path.parent.mkdir(parents=True, exist_ok=True)
            open(intermediate_t_path, "w").close()
            self._subject_algorithm_intermediate_paths[(subject.subject_id, algorithm_name)] = intermediate_t_path
            self.locks[intermediate_t_path] = Lock()

            consolidated_t_path = (
                self.base_dir
                / f"session_{session_index}"
                / subject.subject_id
                / tag_folder
                / "computed"
                / "consolidated"
                / self._build_computed_filename(
                    algorithm=algorithm_name,
                    stage="consolidated_time",
                    activity=safe_tag,
                    timestamp=session_ts,
                )
            )
            consolidated_t_path.parent.mkdir(parents=True, exist_ok=True)
            open(consolidated_t_path, "w").close()
            self._subject_algorithm_consolidated_paths[(subject.subject_id, algorithm_name)] = consolidated_t_path
            self.locks[consolidated_t_path] = Lock()

        for entry in subject.sensors:
            location = entry["meta"].get("location") or "na"
            compute_algo = entry.get("meta", {}).get("compute_algorithm", {}) or {}
            algorithm_name = compute_algo.get("name") or "unknown"
            sensor_address = entry["sensor"].address or "na"

            # RAW CSV
            raw_path = (
                self.base_dir
                / f"session_{session_index}"
                / subject.subject_id
                / tag_folder
                / "raw"
                / f"{location}_{safe_tag}_{session_ts}.csv"
            )
            print(raw_path)
            raw_path.parent.mkdir(parents=True, exist_ok=True)

            # COMPUTED NDJSON (real-time)
            computed_rt_path = (
                self.base_dir
                / f"session_{session_index}"
                / subject.subject_id
                / tag_folder
                / "computed"
                / "real_time"
                / self._build_computed_filename(
                    algorithm=algorithm_name,
                    location=location,
                    address=sensor_address,
                    stage="real_time",
                    activity=safe_tag,
                    timestamp=session_ts,
                )
            )
            computed_rt_path.parent.mkdir(parents=True, exist_ok=True)

            # Create empty raw CSV. Headers are inferred from the first payload shape.
            open(raw_path, "w").close()

            # Create empty NDJSON file
            open(computed_rt_path, "w").close()


            # Store paths
            entry["meta"]["file_path"] = raw_path
            entry["meta"]["real_time_results_file_path"] = computed_rt_path
            entry["meta"]["consolidated_time_results_file_path"] = (
                self._subject_algorithm_consolidated_paths.get((subject.subject_id, str(algorithm_name)))
            )
            entry["meta"]["tag"] = safe_tag
            entry["meta"]["subject_id"] = subject.subject_id
            entry["meta"]["raw_headers"] = None
            pipeline_diagnostics.register_sensor(
                address=sensor_address,
                subject_id=subject.subject_id,
                location=location,
                tag=safe_tag,
            )


            # Register locks
            self.locks[raw_path] = Lock()
            self.locks[computed_rt_path] = Lock()

    def start_session_diagnostics(self, session_index: str | None, metadata: dict | None = None) -> None:
        """Create or refresh the session diagnostics artifact for a session."""
        self.base_dir = self._resolve_base_dir()
        self.base_dir.mkdir(parents=True, exist_ok=True)
        session_ts = str(session_index) if session_index else None
        session_dir = self._session_dir(session_ts)
        if not session_dir:
            return
        diagnostics_dir = session_dir / "diagnostics"
        summary_path = diagnostics_dir / "session_diagnostics.json"
        events_path = diagnostics_dir / "session_diagnostics.jsonl"
        diagnostics_dir.mkdir(parents=True, exist_ok=True)
        now = datetime.now().isoformat(timespec="seconds")
        with self._session_diagnostics_lock:
            current_ts = self._session_diagnostics_state.get("session_timestamp")
            if current_ts != session_ts:
                self._session_diagnostics_state = {
                    "session_timestamp": session_ts,
                    "session_dir": session_dir,
                    "summary_path": summary_path,
                    "events_path": events_path,
                    "summary": {
                        "site": self.site,
                        "session_label": self.session_label,
                        "session_name": self.session_name,
                        "session_timestamp": session_ts,
                        "session_dir": str(session_dir.resolve()),
                        "status": "starting",
                        "started_at": now,
                        "last_updated_at": now,
                        "lifecycle_events": [],
                        "gateway_diagnostics": [],
                        "errors": [],
                    },
                }
            if metadata:
                self._merge_summary(self._session_diagnostics_state["summary"], metadata)
            self._session_diagnostics_state["summary"]["last_updated_at"] = now
            self._write_session_diagnostics_summary_locked()

    def append_session_diagnostics_event(
        self,
        session_index: str | None,
        event_type: str,
        payload: dict | None = None,
    ) -> None:
        """Append a time-ordered diagnostics event and refresh summary state."""
        self.start_session_diagnostics(session_index)
        payload = deepcopy(payload or {})
        now = datetime.now().isoformat(timespec="seconds")
        with self._session_diagnostics_lock:
            if self._session_diagnostics_state.get("session_timestamp") != str(session_index):
                return
            summary = self._session_diagnostics_state.get("summary")
            events_path = self._session_diagnostics_state.get("events_path")
            if not summary or not events_path:
                return
            record = {
                "timestamp": now,
                "type": event_type,
                "payload": payload,
            }
            with events_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record) + "\n")
            summary["last_updated_at"] = now
            if event_type.startswith("stream_"):
                summary["status"] = payload.get("phase", summary.get("status"))
                summary.setdefault("lifecycle_events", []).append(record)
            elif event_type == "sensor_diagnostics":
                summary.setdefault("gateway_diagnostics", []).append(record)
                summary["latest_gateway_diagnostics"] = payload
            elif event_type == "error":
                summary.setdefault("errors", []).append(record)
            self._write_session_diagnostics_summary_locked()

    def update_session_diagnostics_summary(self, session_index: str | None, updates: dict) -> None:
        """Merge structured summary fields into the current session diagnostics summary."""
        self.start_session_diagnostics(session_index)
        now = datetime.now().isoformat(timespec="seconds")
        with self._session_diagnostics_lock:
            if self._session_diagnostics_state.get("session_timestamp") != str(session_index):
                return
            summary = self._session_diagnostics_state.get("summary")
            if not summary:
                return
            self._merge_summary(summary, deepcopy(updates))
            summary["last_updated_at"] = now
            self._write_session_diagnostics_summary_locked()

    def record_session_diagnostics_error(
        self,
        session_index: str | None,
        message: str,
        *,
        context: dict | None = None,
    ) -> None:
        """Record a diagnostics error entry without throwing from file management."""
        payload = {"message": message}
        if context:
            payload["context"] = context
        self.append_session_diagnostics_event(session_index, "error", payload)

    def finalize_session_diagnostics(
        self,
        session_index: str | None,
        *,
        status: str,
        reason: str | None = None,
        summary_updates: dict | None = None,
    ) -> None:
        """Finalize the session diagnostics summary before archive/drain."""
        self.start_session_diagnostics(session_index)
        now = datetime.now().isoformat(timespec="seconds")
        with self._session_diagnostics_lock:
            if self._session_diagnostics_state.get("session_timestamp") != str(session_index):
                return
            summary = self._session_diagnostics_state.get("summary")
            if not summary:
                return
            summary["status"] = status
            summary["finalized_at"] = now
            summary["last_updated_at"] = now
            if reason:
                summary["reason"] = reason
            if summary_updates:
                self._merge_summary(summary, deepcopy(summary_updates))
            self._write_session_diagnostics_summary_locked()

    def describe_session(self, session_index: str | None) -> dict:
        """
        Describe the on-disk session directory layout for a session timestamp.

        Args:
            session_index: Session timestamp/index.

        Returns:
            dict: Session directory metadata.
        """
        session_ts = str(session_index) if session_index else None
        session_dir = self.base_dir / f"session_{session_ts}" if session_ts else None
        return {
            "site": self.site,
            "session_label": self.session_label,
            "session_name": self.session_name,
            "session_timestamp": session_ts,
            "base_root": str(self.base_root.resolve()),
            "base_dir": str(self.base_dir.resolve()),
            "session_dir": str(session_dir.resolve()) if session_dir else None,
            "session_dir_exists": bool(session_dir and session_dir.exists()),
            "session_relative_path": str(session_dir.relative_to(self.base_root)) if session_dir else None,
        }

    def archive_session(self, session_index: str | None) -> dict:
        """
        Archive a finalized session directory locally and remove the raw directory.

        Args:
            session_index: Session timestamp/index.

        Returns:
            dict: Archive metadata to include in events.
        """
        session_info = self.describe_session(session_index)
        session_dir = session_info.get("session_dir")
        if not session_dir:
            raise ValueError("Session directory is unavailable for archiving")

        archive_name = build_session_archive_name(
            site=self.site,
            session_label=session_info.get("session_name"),
            session_timestamp=session_info.get("session_timestamp"),
        )
        archive_path = self.base_dir / archive_name
        result = archive_session_directory(session_dir, archive_path=archive_path, remove_source=True)
        session_info.update(
            {
                "session_archive_name": result.archive_name,
                "session_archive_path": str(result.archive_path),
                "session_archive_exists": result.archive_path.exists(),
                "session_dir_exists": False,
            }
        )
        return session_info


    def stop_stream(self, subject):
        """
        Clear file path metadata for a subject after streaming.

        Args:
            subject: Subject instance.
        """
        for entry in subject.sensors:
            entry["meta"].pop("file_path", None)
            entry["meta"].pop("real_time_results_file_path", None)
            entry["meta"].pop("consolidated_time_results_file_path", None)
            entry["meta"].pop("subject_id", None)
        for key in [k for k in self._subject_algorithm_intermediate_paths if k[0] == subject.subject_id]:
            path = self._subject_algorithm_intermediate_paths.pop(key, None)
            if path:
                self.locks.pop(path, None)
        for key in [k for k in self._subject_algorithm_consolidated_paths if k[0] == subject.subject_id]:
            path = self._subject_algorithm_consolidated_paths.pop(key, None)
            if path:
                self.locks.pop(path, None)

    def enqueue_block(self, entry, samples: List[Any]):
        """
        Queue a completed raw sample block for asynchronous disk writing.

        Args:
            entry: Sensor entry dict with 'sensor' and 'meta'.
            samples: Completed block of samples. Copied to prevent later mutation.
        """
        path = entry["meta"].get("file_path")
        if not path or not samples:
            return
        address = getattr(entry.get("sensor"), "address", None)
        pipeline_diagnostics.increment(address, "raw_blocks_enqueued", 1)
        pipeline_diagnostics.increment(address, "raw_samples_enqueued", len(samples))
        self._raw_write_queue.put((entry, list(samples)))
        pipeline_diagnostics.set_queue_depth(self._raw_write_queue.qsize())

    def flush(self):
        """Block until all queued raw writes are complete."""
        pending = self._raw_write_queue.qsize()
        if pending:
            self.logger.info("flushing raw write queue pending_blocks=%s", pending)
        self._raw_write_queue.join()

    def get_raw_write_failures(self, subject_ids: list[str] | None = None) -> dict[str, list[dict]]:
        with self._raw_write_failures_lock:
            if subject_ids is None:
                return {key: list(value) for key, value in self._raw_write_failures.items()}
            return {
                subject_id: list(self._raw_write_failures.get(subject_id, []))
                for subject_id in subject_ids
                if self._raw_write_failures.get(subject_id)
            }

    def mark_subject_outputs_partial(self, subject_ids: list[str]) -> dict[str, str]:
        """
        Write per-subject marker files for any raw write failures.

        Returns:
            dict mapping subject_id to marker file path.
        """
        failures_by_subject = self.get_raw_write_failures(subject_ids)
        markers = {}
        for subject_id, failures in failures_by_subject.items():
            if not failures:
                continue
            first_path = Path(failures[0]["path"])
            marker_path = first_path.parent.parent / "_partial_incomplete.json"
            marker_payload = {
                "subject_id": subject_id,
                "status": "partial",
                "reason": "raw_write_failure",
                "failure_count": len(failures),
                "failures": failures,
            }
            marker_path.write_text(json.dumps(marker_payload, indent=2), encoding="utf-8")
            markers[subject_id] = str(marker_path)
        return markers

    def write_block(self, entry, samples: List[Any]):
        """
        Thread-safe write of a block of samples to CSV.

        Args:
            entry (dict): Sensor entry dict with 'sensor' and 'meta'.
            samples (List[Any]): List of samples to write.

        Notes:
            - Uses per-file Lock for thread safety.
            - Does nothing if the file path is missing or sample list is empty.
        """
        path = entry["meta"].get("file_path")
        if not path or not samples:
            return

        sample_mappings = [self._sample_to_mapping(sample) for sample in samples]
        with self.locks[path]:
            headers = self._ensure_raw_headers(entry, path, sample_mappings)
            with open(path, "a", newline="") as f:
                writer = csv.writer(f)
                for mapping in sample_mappings:
                    writer.writerow(
                        [self._serialize_csv_value(mapping.get(header)) for header in headers]
                    )


    def _json_serializer(self, obj):
        """
        Serialize objects to JSON-friendly values.

        Args:
            obj: Object to serialize.

        Returns:
            JSON-serializable value.
        """
        if isinstance(obj, Enum):
            return obj.name   # "REAL_TIME"
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        if hasattr(obj, "__dict__"):
            return {key: value for key, value in obj.__dict__.items() if not str(key).startswith("_")}
        if hasattr(obj, "name"):
            return obj.name
        return str(obj)


    # write computed results to json
    def write_computed_json(self, entry, result):
        """
        Append computed results to NDJSON files by stage.

        Args:
            entry: Sensor entry with metadata and paths.
            result: Result object or dict with stage information.
        """
        # normalize stage to lowercase string
        if hasattr(result, "stage"):
            stage_name = result.stage.name.lower() if isinstance(result.stage, Enum) else str(result.stage).lower()
        else:
            stage_name = str(result.get("stage")).lower()

        if stage_name == "real_time":
            path = entry["meta"].get("real_time_results_file_path")
        elif stage_name == "intermediate_time":
            return
        elif stage_name == "consolidated_time":
            path = entry["meta"].get("consolidated_time_results_file_path")
        else:
            return

        if not path:
            return

        with self.locks[path], open(path, "a") as f:
            f.write(json.dumps(result, default=self._json_serializer))
            f.write("\n")

    def write_intermediate_json(self, subject_id: str, algorithm_name: str, result: dict):
        """
        Append intermediate results to a subject+algorithm NDJSON file.

        Args:
            subject_id: Subject identifier.
            algorithm_name: Algorithm name.
            result: Intermediate result dict.
        """
        path = self._subject_algorithm_intermediate_paths.get((subject_id, algorithm_name))
        if not path:
            return
        with self.locks[path], open(path, "a") as f:
            f.write(json.dumps(result, default=self._json_serializer))
            f.write("\n")

    def write_consolidated_json(self, subject_id: str, algorithm_name: str, result: dict):
        """
        Append consolidated results to a subject+algorithm NDJSON file.
        """
        path = self._subject_algorithm_consolidated_paths.get((subject_id, algorithm_name))
        if not path:
            return
        with self.locks[path], open(path, "a") as f:
            f.write(json.dumps(result, default=self._json_serializer))
            f.write("\n")

    def get_subject_intermediate_path(self, subject_id: str, algorithm_name: str | None = None):
        """Return intermediate NDJSON path(s) for a subject."""
        if algorithm_name is not None:
            return self._subject_algorithm_intermediate_paths.get((subject_id, algorithm_name))
        return {
            algo: path
            for (sid, algo), path in self._subject_algorithm_intermediate_paths.items()
            if sid == subject_id
        }

    def read_intermediate_json(self, subject_id: str, algorithm_name: str | None = None):
        """
        Read intermediate NDJSON records for a subject.

        Args:
            subject_id: Subject identifier.
            algorithm_name: Optional algorithm filter.

        Returns:
            List of parsed record dicts.
        """
        records = []
        if algorithm_name is not None:
            candidates = [self._subject_algorithm_intermediate_paths.get((subject_id, algorithm_name))]
        else:
            candidates = [
                path
                for (sid, _algo), path in self._subject_algorithm_intermediate_paths.items()
                if sid == subject_id
            ]
        for path in candidates:
            if not path or not path.exists():
                continue
            lock = self.locks.get(path)
            context = lock if lock is not None else Lock()
            with context, open(path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if algorithm_name and payload.get("algorithm_name") != algorithm_name:
                        continue
                    records.append(payload)
        return records
