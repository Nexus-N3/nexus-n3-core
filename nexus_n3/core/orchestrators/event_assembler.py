"""Event payload assembly for system events."""

from dataclasses import asdict, is_dataclass

from nexus_n3.plugins.runtime.serde import to_jsonable


class EventAssembler:
    """Builds event payloads for compute and intermediate results."""

    def build_compute_payload(self, subject_id, result, location, context=None):
        if isinstance(result, dict):
            result_dict = to_jsonable(result)
        elif is_dataclass(result):
            result_dict = to_jsonable(asdict(result))
        else:
            result_dict = to_jsonable(vars(result))
        address = result_dict.get("address")
        payload = {
            "subject_id": subject_id,
            "result": result_dict,
            "address": address,
            "sensor_id": address,
            "location": location,
            "algorithm_name": result_dict.get("algorithm_name"),
        }
        if context:
            payload.update(context)
        return payload

    def build_intermediate_payload(self, subject_id, algorithm_name, stage, results, context=None):
        payload = {
            "subject_id": subject_id,
            "algorithm_name": algorithm_name,
            "stage": stage,
            "results": results,
        }
        if context:
            payload.update(context)
        return payload
