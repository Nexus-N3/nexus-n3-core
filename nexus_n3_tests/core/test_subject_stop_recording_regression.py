"""Exercise subject STOP through the manager while another subject records."""
import asyncio
import csv
import json
import threading
from types import SimpleNamespace as NS

import pytest

from nexus_n3.core import core as core_module
from nexus_n3.core.orchestrators.event_assembler import EventAssembler
from nexus_n3.gateway.messaging import message_types as mt
from nexus_n3.core.subject import Subject
from nexus_n3.file_manager.FileManager import FileManager
from nexus_n3.sensor_manager.SensorManager import SensorManager
from nexus_n3.sensor_manager import streaming_service
from nexus_n3.sensor_manager.sensor_handle import SensorBase
from nexus_n3.sensor_manager.types.connections import ConnectionStatus
from nexus_n3_tests.core.test_8_subject_stop_archives_once import _make_core


class Sensor(SensorBase):
    def __init__(self, address, subject, consumer=False):
        super().__init__(NS(local_name='Regression Sensor'), {
            'sensor': {'adapter': 'BLE'}, 'events': ['on_data'],
            'inputs': [{'name': subject}] if consumer else [], 'outputs': [],
            'locations': {'supported': [address]},
        })
        self.address = address
        self.connection_status = ConnectionStatus.CONNECTED
        self.measurement_cleanup_pending = not consumer
        self.routing_outputs = [] if consumer else [{'name': subject}]
        self.values = []
    def consume_input(self, plugin_id, envelope):
        self.values.append(envelope.payload.value * 2)
        callback = getattr(self, "result_callback", None)
        if callback is not None:
            callback({"address": envelope.source_address, "value": self.values[-1],
                      "algorithm_name": "regression", "stage": "real_time", "timestamp": envelope.payload.timestamp})
    async def stop_stream(self, adapter): return None


class Adapter:
    def close(self): pass
    async def disconnect(self, client):
        client.is_connected = False
        return True


@pytest.mark.parametrize('outcome,recover_after_stop', [
    ('success', False), ('false', False), ('exception', False), ('timeout', False),
    ('false', True), ('exception', True), ('timeout', True),
])
def test_other_subject_keeps_samples_csv_and_compute_during_and_after_stop(tmp_path, monkeypatch, outcome, recover_after_stop):
    monkeypatch.setattr(core_module.time, 'sleep', lambda _: None)
    monkeypatch.setattr(streaming_service, 'SENSOR_STOP_TIMEOUT_S', .2 if outcome == 'timeout' else 2)
    monkeypatch.setattr('nexus_n3.sensor_manager.SensorManager.utils.remove_all_devices', lambda: None)
    monkeypatch.setattr('nexus_n3.sensor_manager.adapter_pool.resolve_adapter_class', lambda *_: Adapter)
    manager = SensorManager()
    subjects, sources, consumers = [], [], []
    for number in (1, 2):
        subject = Subject(f'subject{number}', [])
        subject.is_streaming = True
        sensors = [Sensor(f'{number}-{index}', subject.subject_id) for index in (1, 2)]
        for sensor in sensors:
            subject.add_sensor(sensor, {'location': sensor.address, 'f_block_size': 1})
        subjects.append(subject)
        sources.extend(sensors)
        consumers.append(Sensor(f'consumer{number}', subject.subject_id, consumer=True))
    manager.init_sensor_manager([entry for subject in subjects for entry in subject.sensors] + consumers)
    manager.enable_routing()
    core, _, events = _make_core(subjects)
    files = FileManager('stop-regression', base_dir=tmp_path)
    for subject in subjects: files.start_stream(subject, core.session_timestamp)
    raw_path = subjects[1].sensors[0]['meta']['file_path']
    core.storage = NS(file_manager=files)
    core.sensor_orch = NS(manager=manager, stop_specific=manager.stop_specific_sensors, stop_all=manager.stop_all)
    compute_samples = []
    core.compute_orch = NS(ingest_sample=lambda sample, **_: compute_samples.append(sample.value),
                           run_consolidation=lambda *_: [])
    core.subject_graph = NS(find_subject_by_address=lambda address: next(
        (subject for subject in subjects if any(entry['sensor'].address == address for entry in subject.sensors)), None))
    core.event_assembler = EventAssembler()
    consumers[1].result_callback = core._on_compute_result
    computed_path = subjects[1].sensors[0]['meta']['real_time_results_file_path']
    core._official_outputs_active = True
    core._official_start_mode = 'local'
    core._official_stream_origin_monotonic_ns = 123
    manager.register_listener('on_data', core._on_data)
    started, release = threading.Event(), threading.Event()
    attempted = []
    async def first_stop(_):
        attempted.append(sources[0].address)
        started.set()
        if outcome == 'timeout': await asyncio.Event().wait()
        assert await asyncio.to_thread(release.wait, 2)
        if outcome == 'false': return False
        if outcome == 'exception': raise RuntimeError('injected STOP failure')
    async def second_stop(_): attempted.append(sources[1].address)
    sources[0].stop_stream, sources[1].stop_stream = first_stop, second_stop
    failures = []
    def finish():
        try: core._stop_stream_impl([subjects[0]], stop_specific=True)
        except Exception as exc: failures.append(exc)
    def acquire(value):
        sample = NS(address=sources[2].address, value=value, timestamp=value, sampling_rate=60)
        manager._emit_to_client('on_data', sample)
        manager._wait_for_routing_to_drain([sources[2].address])
        files.flush()
    worker = threading.Thread(target=finish)
    try:
        acquire(1)
        worker.start()
        assert started.wait(2)
        acquire(2)  # STOP is outstanding, not merely after it completes.
        release.set()
        worker.join(3)
        assert not worker.is_alive() and not failures
        acquire(3)
        assert subjects[0].is_streaming is False
        assert subjects[1].is_streaming is True
        assert core.stream_phase == 'official_streaming'
        assert core._official_outputs_active
        assert core._official_stream_origin_monotonic_ns == 123
        assert compute_samples == [1, 2, 3]
        assert consumers[1].values == [2, 4, 6]
        with raw_path.open() as stream:
            assert [int(row['value']) for row in csv.DictReader(stream)] == [1, 2, 3]
        assert attempted == [sources[0].address, sources[1].address]
        assert [json.loads(line)['value'] for line in computed_path.read_text().splitlines()] == [2, 4, 6]
        drained = next(event['payload'] for event in reversed(events.events) if event['type'] == mt.EVT_STREAM_DRAINED)
        assert drained['physical_stop_complete'] == (outcome == 'success')
        assert [r['status'] for r in drained['physical_stop_results']] == [
            'stopped' if outcome == 'success' else 'unconfirmed', 'stopped']
        # Stopped-subject samples do not enter persistence or compute.
        manager._emit_to_client('on_data', NS(address=sources[0].address, value=99, timestamp=99))
        assert compute_samples == [1, 2, 3]
        if recover_after_stop:
            started.clear()
            release.clear()
            sources[0].transport_client = NS(is_connected=True)
            async def recovery_stop(_):
                started.set()
                assert await asyncio.to_thread(release.wait, 2)
            sources[0].stop_stream = recovery_stop
            recovery = manager.recover_sensors([sources[0].address])
            assert started.wait(2)
            acquire(4)  # Recovery is pending while the other subject records.
            release.set()
            assert recovery.result(3)['results'][0]['measurement_stop_confirmed']
            acquire(5)
            assert compute_samples == [1, 2, 3, 4, 5]
            with raw_path.open() as stream:
                assert [int(row['value']) for row in csv.DictReader(stream)] == [1, 2, 3, 4, 5]
            assert [json.loads(line)['value'] for line in computed_path.read_text().splitlines()] == [2, 4, 6, 8, 10]
            assert core.stream_phase == 'official_streaming' and subjects[1].is_streaming
            core._on_stream_stopped([sources[0].address])
            assert events.events[-1]['payload']['subject_ids'] == [subjects[0].subject_id]
        core._stop_stream_impl([subjects[1]], stop_specific=True)
        assert core.stream_phase == 'idle'
        assert events.events[-1]['payload']['session_archive_exists']
        assert events.events[-1]['payload']['physical_stop_complete'] == (outcome == 'success')
    finally:
        release.set()
        if worker.is_alive(): worker.join(3)
        manager.stop_manager()
        manager.loop.close()
