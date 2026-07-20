"""Subject model for grouping sensors and writing data blocks."""

import time
from collections import defaultdict
from nexus_n3.core.pipeline_diagnostics import pipeline_diagnostics


def _result_address(result):
    if isinstance(result, dict):
        return result.get("address")
    return getattr(result, "address", None)

class Subject:
    """
    Represents a subject (e.g., a person) with associated sensors and collected data.

    Each subject has:
        - A unique `subject_id`.
        - A list of sensor instances with optional metadata.
        - A data buffer for storing incoming sensor samples.
        - Sample count tracking.

    Attributes:
        subject_id (str): Unique identifier for the subject.
        sensor_configs (list[dict]): Configuration for each sensor assigned to the subject.
        sensors (list[dict]): List of dicts with keys 'sensor' (sensor object) and 'meta' (metadata).
        data (defaultdict[list]): Stores collected samples by sensor.
        sample_count (int): Total number of samples ingested for this subject.
    """

    def __init__(self, subject_id, sensor_configs):
        """
        Initialize a Subject instance.

        Args:
            subject_id (str): Unique identifier for the subject.
            sensor_configs (list[dict]): Sensor configurations, typically containing 'local_name' and 'number_of'.
        """
        self.subject_id = subject_id
        self.sensor_configs = sensor_configs
        self.sensors = []  # Each entry: {"sensor": sensor_obj, "meta": {...}}
        self.data = defaultdict(list) 
        self.sample_count = 0
        self.is_streaming = False

    def add_sensor(self, sensor, meta_data=None):
        """
        Add a sensor instance to this subject.

        Args:
            sensor: Sensor object instance.
            meta_data (dict, optional): Metadata such as body location, block size, etc.
                Defaults to {"location": None}.
        """
        if meta_data is None:
            meta_data = {"location": None}
        sensor.raw_data = []
        self.sensors.append({"sensor": sensor, "meta": meta_data})

    def ingest_sample(self, sample, file_manager):
        """
        Ingest a sample from a sensor, store it in the sensor buffer, and enqueue blocks to file if ready.

        Args:
            sample: Sample object containing at least an 'address' attribute to identify the sensor.
            file_manager: FileManager instance used to queue completed data blocks.

        Returns:
            bool: True if the sample was successfully ingested; False if the sensor was not found.
        """
        self.sample_count += 1 # not really needed
        entry = next((e for e in self.sensors if e["sensor"].address == sample.address), None)
        if not entry:
            return False

        sensor = entry["sensor"]
        sensor.raw_data.append(sample)
        block_size = entry["meta"].get("f_block_size", 300)
        pipeline_diagnostics.increment(
            sample.address,
            "ingest_count",
            1,
            subject_id=self.subject_id,
            location=entry["meta"].get("location"),
            tag=entry["meta"].get("tag"),
        )

        # Hand complete blocks to the background writer so the callback path stays lightweight.
        while len(sensor.raw_data) >= block_size:
            block = sensor.raw_data[:block_size]
            sensor.raw_data = sensor.raw_data[block_size:]
            file_manager.enqueue_block(entry, block)

        return True

    def ingest_result(self, result, file_manager):
        """
        Write a computed result for a sensor to storage.

        Args:
            result: Result object with address and stage information.
            file_manager: FileManager instance.

        Returns:
            True if written, False if the sensor entry was not found.
        """
        result_address = _result_address(result)
        entry = next((e for e in self.sensors if e["sensor"].address == result_address), None)
        if not entry:
            print("no entry found")
            return False

        #result_dict = asdict(result)
        #print(f"writing result for stage {result_dict['stage']}")
        file_manager.write_computed_json(entry, result)

        return True
    
    def ingest_intermediate_result(self, result: dict, file_manager):
        """
        Write intermediate results to file for this subject.
        """
        subject_results = []
        subject_addresses = {entry["sensor"].address for entry in self.sensors}
        for entry in result.get("results", []):
            addr = entry.get("address")
            if addr and addr in subject_addresses:
                subject_results.append(entry)
            elif entry.get("subject_id") == self.subject_id:
                subject_results.append(entry)

        if not subject_results:
            return False

        filtered_result = {
            "algorithm_name": result.get("algorithm_name"),
            "stage": result.get("stage"),
            "results": subject_results,
        }
        algo_name = str(result.get("algorithm_name") or "")
        if not algo_name:
            return False
        file_manager.write_intermediate_json(self.subject_id, algo_name, filtered_result)

        return True



    def set_location(self, address, location):
        """
        Assign a body location to a sensor by its address.

        This is useful if sensors are assigned dynamically (e.g., by button press).

        Args:
            address (str): Address of the sensor to update.
            location (str): Body location to assign (e.g., 'left_ankle').

        Returns:
            dict: The sensor entry after updating its metadata.

        Raises:
            ValueError: If no sensor with the given address exists for this subject.
        """
        entry = next((s for s in self.sensors if s["sensor"].address == address), None)
        if not entry:
            raise ValueError(f"No sensor with address {address} for {self.subject_id}")
        entry["meta"]["location"] = location
        return entry
