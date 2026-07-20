"""Consolidation-stage executor registry and dispatch."""

from __future__ import annotations

import threading


class ConsolidationStage:
    """Owns consolidation executors and executes them per subject/algorithm."""

    def __init__(self, error_cb=None):
        self.error_cb = error_cb
        self._consolidation_executors = {}
        self._lock = threading.Lock()

    def register_consolidation_executor(self, algorithm_name, executor):
        """Register a consolidation executor for an algorithm."""
        with self._lock:
            self._consolidation_executors[algorithm_name] = executor

    def reset(self):
        """Clear consolidation executor registry."""
        with self._lock:
            self._consolidation_executors.clear()

    def run_for_subject(self, subject_id: str, algorithm_name: str, intermediate_records: list[dict]):
        """
        Run the consolidation executor (if present) for the subject/algorithm.

        Returns:
            dict | None: Consolidated payload dict or None.
        """
        with self._lock:
            executor = self._consolidation_executors.get(algorithm_name)
        if not executor:
            return None
        try:
            return executor.consolidate(
                subject_id=subject_id,
                intermediate_records=intermediate_records,
            )
        except Exception as exc:
            if self.error_cb:
                self.error_cb(f"Consolidation executor failed for {algorithm_name}: {exc}")
            return None

