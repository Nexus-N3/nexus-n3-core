"""Intermediate-stage buffering and executor scheduling."""

import threading
from collections import defaultdict, deque


class IntermediateStage:
    """Owns intermediate executors and per-algorithm result buffers."""

    def __init__(self, max_results_per_stream=1000, error_cb=None):
        self.max_results_per_stream = max_results_per_stream
        self.error_cb = error_cb
        self._intermediate_executors = {}
        self._results = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=self.max_results_per_stream))
        )
        self._lock = threading.Lock()

    def register_intermediate_executor(self, algorithm_name, executor):
        """Register an intermediate executor for an algorithm."""
        with self._lock:
            self._intermediate_executors[algorithm_name] = executor

    def reset(self):
        """Clear executor and buffer state."""
        with self._lock:
            self._intermediate_executors.clear()
            self._results.clear()

    def handle_result(self, result):
        """
        Buffer a result and run intermediate executor if ready.

        Returns:
            Intermediate result object, or None.
        """
        algo_name = result.algorithm_name
        with self._lock:
            self._results[algo_name][result.address].append(result)
            executor = self._intermediate_executors.get(algo_name)
            if not executor:
                return None
            if not executor.should_run(self._results[algo_name]):
                return None
            try:
                return executor.run(self._results[algo_name])
            except Exception as exc:
                if self.error_cb:
                    self.error_cb(f"Intermediate executor failed for {algo_name}: {exc}")
                return None

    def get_results(self, algorithm_name, address=None, limit=None):
        """Retrieve buffered results for an algorithm."""
        with self._lock:
            if address:
                results = list(self._results[algorithm_name][address])
            else:
                results = [
                    r
                    for addr_results in self._results[algorithm_name].values()
                    for r in addr_results
                ]

        if limit:
            return results[-limit:]
        return results
