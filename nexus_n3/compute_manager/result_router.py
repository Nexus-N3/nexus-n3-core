"""Result fanout and intermediate-stage routing."""

import threading


class ResultRouter:
    """Route per-sensor results to listeners and intermediate stage."""

    def __init__(self, intermediate_stage):
        self._intermediate_stage = intermediate_stage
        self._lock = threading.Lock()
        self._algorithm_result_listener = None
        self._intermediate_result_listener = None

    def register_result_listener(self, callback):
        """Register callback for per-sensor results."""
        with self._lock:
            self._algorithm_result_listener = callback

    def register_intermediate_result_listener(self, callback):
        """Register callback for intermediate aggregated results."""
        with self._lock:
            self._intermediate_result_listener = callback

    def handle_result(self, result):
        """Handle one algorithm result and fan out to listeners."""
        with self._lock:
            algo_listener = self._algorithm_result_listener
            intermediate_listener = self._intermediate_result_listener

        if algo_listener:
            algo_listener(result)

        intermediate_result = self._intermediate_stage.handle_result(result)
        if intermediate_result and intermediate_listener:
            intermediate_listener(intermediate_result)
