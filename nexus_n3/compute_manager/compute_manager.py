"""Compute manager for running per-sensor algorithms and aggregations."""

import threading
import os
import time
from queue import Queue, Empty
from collections import defaultdict

from nexus_n3.compute_manager.intermediate_stage import IntermediateStage
from nexus_n3.compute_manager.consolidation_stage import ConsolidationStage
from nexus_n3.compute_manager.result_router import ResultRouter
from nexus_n3.compute_manager.remote_compute_service import RemoteComputeService
from nexus_n3.logger.logger import get_module_logger

logger = get_module_logger("Compute Manager")

class ComputeManager:
    """
    Execute per-sensor algorithms and optional intermediate executors.

    The manager receives samples from the core, runs algorithm instances
    per sensor address, and forwards results to listeners and storage.
    """

    def __init__(self, system_event_bus=None, error_cb=None):
        """
        Initialize the compute manager.

        Args:
            system_event_bus: Optional event bus for system-wide events.
            error_cb: Optional callback for error reporting.
        """
        self.system_event_bus = system_event_bus
        self.error_cb = error_cb
        self._algorithms = {}  # address -> algorithm

        self._queue = Queue()

        # listeners from the system (retained for compatibility)
        self.on_algorithm_result_listener = None
        self.on_intermeidate_result_listener = None
        self.on_intermediate_result_listener = None

        # 1000 blocks is 5000 seconds which is about 83 minutes?
        # it might be an idea to use the written files and delete the
        # buffer after each intermediate execution 
        self.max_results_per_stream = 1000
        self._intermediate_stage = IntermediateStage(
            max_results_per_stream=self.max_results_per_stream,
            error_cb=self.error_cb,
        )
        self._consolidation_stage = ConsolidationStage(
            error_cb=self.error_cb,
        )
        self._result_router = ResultRouter(self._intermediate_stage)
        self._remote_service = RemoteComputeService(
            on_result=self.on_algorithm_result,
            error_cb=self.error_cb,
        )
        # compatibility aliases for any legacy direct access
        self._intermediate_executors = self._intermediate_stage._intermediate_executors
        self._results = self._intermediate_stage._results
        self._consolidation_executors = self._consolidation_stage._consolidation_executors

        self._worker = threading.Thread(
            target=self._run,
            daemon=True
        )
        self._worker.start()

        self._perf_enabled = _env_flag("NEXUS_PERF_LOG", default=False)
        self._perf_period_seconds = 10.0
        self._perf_lock = threading.Lock()
        self._perf_samples = defaultdict(int)
        self._perf_windows = defaultdict(lambda: defaultdict(int))
        self._perf_total_samples = 0
        self._perf_total_windows = 0
        if self._perf_enabled:
            self._perf_thread = threading.Thread(
                target=self._perf_loop,
                daemon=True
            )
            self._perf_thread.start()

    # ------------------------
    # Registration
    # ------------------------
    def register_algorithm(self, address, algorithm):
        """
        Register an algorithm instance for a specific sensor address.

        Args:
            address: Sensor address used as the algorithm key.
            algorithm: Algorithm instance implementing on_sample().
        """
        logger.info(f"compute manager registering algo {algorithm} for {address}")
        self._algorithms[address] = algorithm

    def has_algorithm(self, address):
        """Return True if an algorithm is already registered for this address."""
        return address in self._algorithms

    def set_registry(self, registry):
        """Attach a NodeRegistry for AI node discovery."""
        self._remote_service.set_registry(registry)

    def register_intermediate_executor(self, algorithm_name, executor):
        """
        Register an intermediate executor for an algorithm name.

        Args:
            algorithm_name: Algorithm name to register.
            executor: Executor instance that aggregates results.
        """
        self._intermediate_stage.register_intermediate_executor(algorithm_name, executor)

    def register_result_listener(self, callback):
        """
        Register a callback for per-sensor results.

        Args:
            callback: Callable that accepts a result object.
        """
        logger.info("system interface result callack regsitered")
        self.on_algorithm_result_listener = callback
        self._result_router.register_result_listener(callback)

    def register_intermediate_result_listener(self, callback):
        """
        Register a callback for intermediate aggregated results.

        Args:
            callback: Callable that accepts an aggregated result dict.
        """
        self.on_intermeidate_result_listener = callback
        self.on_intermediate_result_listener = callback
        self._result_router.register_intermediate_result_listener(callback)

    def register_consolidation_executor(self, algorithm_name, executor):
        """
        Register a consolidation executor for an algorithm name.

        Args:
            algorithm_name: Algorithm name to register.
            executor: Executor instance used for end-of-stream consolidation.
        """
        self._consolidation_stage.register_consolidation_executor(algorithm_name, executor)

    def reset(self):
        """Clear algorithm/executor state and buffered results."""
        self._algorithms.clear()
        self._intermediate_stage.reset()
        self._consolidation_stage.reset()
        try:
            while True:
                self._queue.get_nowait()
        except Empty:
            pass
    # ------------------------
    # Ingestion
    # ------------------------
    def ingest_sample(self, sample):
        """
        Enqueue a sample for algorithm processing.

        Args:
            sample: Sensor sample with address and data payload.
        """
        if self._perf_enabled:
            try:
                with self._perf_lock:
                    self._perf_samples[sample.address] += 1
                    self._perf_total_samples += 1
            except Exception:
                pass
        self._queue.put(sample)

    # ------------------------
    # Remote compute delegation
    # ------------------------
    def delegate_compute(self, algorithm, samples):
        """
        Delegate compute to an AI node if available.

        Returns:
            bool: True if delegated, False to fall back to local compute.
        """
        return self._remote_service.delegate_compute(algorithm, samples)

    # ------------------------
    # Worker loop
    # ------------------------
    def _run(self):
        """Worker loop that processes samples and dispatches to algorithms."""
        while True:
            sample = self._queue.get()

            algo = self._algorithms.get(sample.address)
            if algo:
                try:
                    algo.on_sample(sample)
                except Exception as e:
                    if self.error_cb:
                        self.error_cb(
                            f"Error processing sample from {sample.address}: {e}"
                        )

    # ------------------------
    # Result handling
    # ------------------------
    def on_algorithm_result(self, result):
        """
        Receive a result from an algorithm and fan it out.

        Args:
            result: Algorithm result object.
        """
        algo_name = result.algorithm_name

        if self._perf_enabled:
            try:
                with self._perf_lock:
                    self._perf_windows[algo_name][result.address] += 1
                    self._perf_total_windows += 1
            except Exception:
                pass

        self._result_router.handle_result(result)

    def on_remote_result(self, result, request_id=None):
        """
        Receive a remote result and enforce local result counting.
        """
        self._remote_service.on_remote_result(result, request_id=request_id)


    # ------------------------
    # Query API (for intermediate stage)
    # ------------------------
    def get_results(self, algorithm_name, address=None, limit=None):
        """
        Retrieve stored results for an algorithm.

        Args:
            algorithm_name: Algorithm name to query.
            address: Optional sensor address filter.
            limit: Optional max number of results.

        Returns:
            List of result objects.
        """
        return self._intermediate_stage.get_results(
            algorithm_name=algorithm_name,
            address=address,
            limit=limit,
        )

    def run_consolidation_for_subject(self, subject_id: str, algorithm_name: str, intermediate_records: list[dict]):
        """
        Run end-of-stream consolidation for one subject+algorithm.

        Returns:
            dict | None: Consolidated payload or None.
        """
        return self._consolidation_stage.run_for_subject(
            subject_id=subject_id,
            algorithm_name=algorithm_name,
            intermediate_records=intermediate_records,
        )

    def _perf_loop(self):
        """Emit periodic performance summaries."""
        while True:
            time.sleep(self._perf_period_seconds)
            try:
                with self._perf_lock:
                    samples = dict(self._perf_samples)
                    windows = {
                        algo: dict(addr_counts)
                        for algo, addr_counts in self._perf_windows.items()
                    }
                    total_samples = self._perf_total_samples
                    total_windows = self._perf_total_windows
                    self._perf_samples = defaultdict(int)
                    self._perf_windows = defaultdict(lambda: defaultdict(int))
                    self._perf_total_samples = 0
                    self._perf_total_windows = 0
                sample_rates_hz = {
                    addr: round(count / self._perf_period_seconds, 3)
                    for addr, count in samples.items()
                }
                logger.info(
                    "perf summary: samples=%s sample_rates_hz=%s windows=%s total_samples=%s total_windows=%s queue=%s",
                    samples,
                    sample_rates_hz,
                    windows,
                    total_samples,
                    total_windows,
                    self._queue.qsize(),
                )
            except Exception:
                pass

def _env_flag(name, default=False):
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}
