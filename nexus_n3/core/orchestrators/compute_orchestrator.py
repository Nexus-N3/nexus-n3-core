"""Compute coordination for algorithm registration and ingestion."""

from nexus_n3.logger.logger import get_module_logger
from nexus_n3.compute_manager.compute_manager import ComputeManager
from nexus_n3.plugins.runtime.runtime import (
    HostBackedAlgorithm,
    HostBackedConsolidationExecutor,
    HostBackedIntermediateExecutor,
    PluginRuntimeManager,
)

logger = get_module_logger("ComputeOrchestrator")


class ComputeOrchestrator:
    """Owns ComputeManager and algorithm registration."""

    def __init__(self, system_event_bus=None, error_cb=None, plugin_root=None):
        self.error_cb = error_cb
        self.compute_manager = ComputeManager(system_event_bus, error_cb)
        self._plugin_runtime = PluginRuntimeManager(plugin_root)
        self._registered_executors = set()
        self._registered_consolidation_executors = set()

    def set_registry(self, registry):
        self.compute_manager.set_registry(registry)

    def register_listeners(self, on_compute_result, on_intermediate_result):
        self.compute_manager.register_result_listener(on_compute_result)
        self.compute_manager.register_intermediate_result_listener(on_intermediate_result)

    def reset(self):
        self.compute_manager.reset()
        self._plugin_runtime.close()
        self._registered_executors.clear()
        self._registered_consolidation_executors.clear()

    def ingest_sample(self, sample):
        self.compute_manager.ingest_sample(sample)

    def register_algorithms(self, subjects):
        for sub in subjects:
            for entry in sub.sensors:
                sensor = entry["sensor"]
                if sensor.address is None:
                    continue

                compute_algo = entry["meta"].get("compute_algorithm", {})
                algo_name = compute_algo.get("name")
                algo_input_parameters = compute_algo.get("inputs")

                if not algo_name:
                    continue

                runtime_client = self._plugin_runtime.get_algorithm_client(algo_name)
                if runtime_client is None:
                    msg = (
                        f"Failed to resolve algorithm plugin '{algo_name}' for "
                        f"sensor {sensor.address}: plugin is not installed or not enabled"
                    )
                    logger.error(msg)
                    if self.error_cb:
                        self.error_cb(msg)
                    continue

                if algo_name not in self._registered_executors:
                    self.compute_manager.register_intermediate_executor(
                        algo_name,
                        HostBackedIntermediateExecutor(runtime_client),
                    )
                    logger.info("Registered host-backed intermediate executor for %s", algo_name)
                    self._registered_executors.add(algo_name)
                if algo_name not in self._registered_consolidation_executors:
                    self.compute_manager.register_consolidation_executor(
                        algo_name,
                        HostBackedConsolidationExecutor(runtime_client),
                    )
                    logger.info("Registered host-backed consolidation executor for %s", algo_name)
                    self._registered_consolidation_executors.add(algo_name)

                if not self.compute_manager.has_algorithm(sensor.address):
                    runtime_client.start_algorithm(
                        address=sensor.address,
                        sampling_rate=sensor.attributes.get("SAMPLING_RATE"),
                        input_parameters=algo_input_parameters,
                        subject_id=sub.subject_id,
                        location=entry["meta"].get("location"),
                    )
                    algo_instance = HostBackedAlgorithm(runtime_client, address=sensor.address)
                    if hasattr(algo_instance, "register_result_listener"):
                        algo_instance.register_result_listener(self.compute_manager.on_algorithm_result)
                    else:
                        algo_instance.result_callback = self.compute_manager.on_algorithm_result

                    self.compute_manager.register_algorithm(sensor.address, algo_instance)
                    logger.info("Registered %s for %s", algo_name, sensor.address)

    def run_consolidation(self, subjects, file_manager):
        """
        Run consolidation executors for the provided subjects using persisted
        intermediate results.

        Returns:
            List[dict]: Consolidated payloads ready for event emission.
        """
        emitted_payloads = []
        for sub in subjects:
            subject_algorithms = set()
            for entry in sub.sensors:
                compute_algo = entry.get("meta", {}).get("compute_algorithm", {}) or {}
                algo_name = compute_algo.get("name")
                if algo_name:
                    subject_algorithms.add(algo_name)

            for algo_name in sorted(subject_algorithms):
                records = file_manager.read_intermediate_json(
                    subject_id=sub.subject_id,
                    algorithm_name=algo_name,
                )
                if not records:
                    continue
                consolidated = self.compute_manager.run_consolidation_for_subject(
                    subject_id=sub.subject_id,
                    algorithm_name=algo_name,
                    intermediate_records=records,
                )
                if not isinstance(consolidated, dict):
                    continue
                emitted_payloads.append(consolidated)
                file_manager.write_consolidated_json(
                    subject_id=sub.subject_id,
                    algorithm_name=str(algo_name),
                    result=consolidated,
                )
        return emitted_payloads
