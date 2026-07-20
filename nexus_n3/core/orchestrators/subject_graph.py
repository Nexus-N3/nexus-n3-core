"""Subject graph coordination for subjects and sensor assignment."""

from nexus_n3.logger.logger import get_module_logger
from nexus_n3.core.subject import Subject
from nexus_n3.plugins.runtime.sensor_runtime import resolve_installed_sensor_class

logger = get_module_logger("SubjectGraph")


class SubjectGraph:
    """Manage subjects, sensors, and lookup helpers."""

    def __init__(self, system_event_bus=None, max_total_sensors=8):
        self.system_event_bus = system_event_bus
        self.max_total_sensors = max_total_sensors
        self.subjects = []

    def init_subjects(self, subjects_config, error_cb=None):
        """Initialize subjects and assign sensors to locations."""
        self.subjects = []
        total_sensors_all_subjects = sum(
            sum(s.get("number_of", 1) for s in conf.get("sensors", []))
            for conf in subjects_config
        )

        if total_sensors_all_subjects > self.max_total_sensors:
            msg = (
                f"Total sensors across all subjects ({total_sensors_all_subjects}) "
                f"exceed the maximum allowed ({self.max_total_sensors})"
            )
            if self.system_event_bus:
                self.system_event_bus.emit({"type": "error", "payload": msg})
            if error_cb:
                error_cb(msg)
            return False

        for conf in subjects_config:
            subject_id = conf["subject_id"]
            sensors_conf = conf.get("sensors", [])

            all_locations = []
            for s_conf in sensors_conf:
                num = s_conf.get("number_of", 1)
                locations = s_conf.get("locations", [])
                if len(locations) != num:
                    msg = (
                        f"Subject '{subject_id}' sensor '{s_conf.get('local_name')}' "
                        f"has {num} sensors but {len(locations)} locations"
                    )
                    if self.system_event_bus:
                        self.system_event_bus.emit({"type": "error", "payload": msg})
                    if error_cb:
                        error_cb(msg)
                    return False
                all_locations.extend(locations)

            if len(all_locations) != len(set(all_locations)):
                msg = f"Subject '{subject_id}' has duplicate body locations: {all_locations}"
                if self.system_event_bus:
                    self.system_event_bus.emit({"type": "error", "payload": msg})
                if error_cb:
                    error_cb(msg)
                return False

            sub = Subject(subject_id, sensors_conf)
            for s_conf in sensors_conf:
                cls = resolve_installed_sensor_class(s_conf["local_name"])
                if cls is None:
                    msg = f"Unsupported sensor plugin: {s_conf.get('local_name')}"
                    if self.system_event_bus:
                        self.system_event_bus.emit({"type": "error", "payload": msg})
                    if error_cb:
                        error_cb(msg)
                    return False
                num = s_conf.get("number_of", 1)
                algo = s_conf.get("compute_algorithm") or {"name": "pass_through", "inputs": {}}
                algo_name = algo.get("name")
                requested_algo = str(algo_name or "").strip().lower()

                # Validate algorithm/sensor compatibility at init time.
                # pass_through is intentionally allowed as a generic fallback.
                if requested_algo and requested_algo != "pass_through":
                    supported_algorithms = []
                    try:
                        raw_spec = cls.load_raw_spec()
                        supported_algorithms = list(raw_spec.get("computations", []) or [])
                    except Exception:
                        supported_algorithms = []

                    supported_normalized = {
                        str(name).strip().lower()
                        for name in supported_algorithms
                        if name
                    }
                    if requested_algo not in supported_normalized:
                        msg = (
                            f"Subject '{subject_id}' sensor '{s_conf.get('local_name')}' "
                            f"cannot run algorithm '{algo_name}'. "
                            f"Supported: {supported_algorithms or ['pass_through']}"
                        )
                        if self.system_event_bus:
                            self.system_event_bus.emit({"type": "error", "payload": msg})
                        if error_cb:
                            error_cb(msg)
                        return False

                locations = s_conf.get("locations", [])
                loc_index = 0
                for _ in range(num):
                    try:
                        sensor = cls(None)
                        location = locations[loc_index]

                        override_attrs = s_conf.get("attributes", {}) or {}
                        if override_attrs:
                            sensor.attributes.update(override_attrs)

                        rate = sensor.attributes.get("SAMPLING_RATE") or 1
                        f_block_size = int(rate * 5)

                        loc_index += 1
                        sub.add_sensor(
                            sensor,
                            meta_data={
                                "location": location,
                                "f_block_size": f_block_size,
                                "compute_algorithm": algo,
                            },
                        )
                        logger.info(
                            "Assigned %s to %s with algo %s",
                            s_conf["local_name"],
                            location,
                            algo_name,
                        )
                    except Exception as exc:
                        msg = f"Sensor init failed for '{s_conf.get('local_name')}': {exc}"
                        logger.exception("Failed to init sensor '%s'", s_conf.get("local_name"))
                        if self.system_event_bus:
                            self.system_event_bus.emit({"type": "error", "payload": msg})
                        if error_cb:
                            error_cb(msg)
                        return False

            self.subjects.append(sub)
        return True

    def get_subjects(self):
        return self.subjects

    def get_subjects_by_ids(self, subject_ids):
        return [s for s in self.subjects if s.subject_id in subject_ids]

    def find_subject_by_address(self, address):
        return next(
            (sub for sub in self.subjects if any(entry["sensor"].address == address for entry in sub.sensors)),
            None
        )

    def find_subject_by_id(self, subject_id):
        return next((s for s in self.subjects if s.subject_id == subject_id), None)

    def find_location_by_address(self, address):
        subject = self.find_subject_by_address(address)
        if not subject:
            return None, None
        entry = next((e for e in subject.sensors if e["sensor"].address == address), None)
        return subject, entry["meta"].get("location") if entry else None
