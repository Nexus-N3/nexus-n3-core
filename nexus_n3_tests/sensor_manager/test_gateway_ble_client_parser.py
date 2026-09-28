import logging
from collections import deque

import pytest

from nexus_n3.sensor_manager.adapters import gateway_ble_client
from nexus_n3.sensor_manager.adapters.gateway_ble_client import (
    GatewaySerialClient,
    SERIAL_READ_HISTORY_SIZE,
    STREAM_FRAME_MAGIC,
    SerialReadCapture,
)
from nexus_n3.sensor_manager.ble_runtime_config import BLERuntimeConfig


def test_drop_and_resync_warns_with_bytes_captured_before_deletion(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    client = GatewaySerialClient(BLERuntimeConfig(backend="gateway"))
    dropped = b"\xde\xad\xbe\xef"
    remaining = bytes(range(80))
    client.buf.extend(dropped + remaining)
    monkeypatch.setattr(gateway_ble_client.time, "monotonic_ns", lambda: 123456789)

    gateway_ble_client.logger.addHandler(caplog.handler)
    try:
        client._drop_and_resync(len(dropped))
    finally:
        gateway_ble_client.logger.removeHandler(caplog.handler)

    assert client.buf == remaining
    assert client.stream_resync_drop_bytes == len(dropped)
    assert client.stream_resync_events == 1
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == logging.WARNING
    assert record.getMessage() == (
        "GATEWAY PARSER RESYNC "
        f"drop_len={len(dropped)} "
        f"buffer_len={len(dropped) + len(remaining)} "
        f"dropped_hex={dropped.hex()} "
        f"remaining_head_hex={remaining[:64].hex()} "
        "monotonic_ns=123456789"
    )


def _stream_frame(payload: bytes, *, sensor_id: int = 7, timestamp_us: int = 123) -> bytes:
    body = (
        bytes((1, sensor_id))
        + timestamp_us.to_bytes(8, "little")
        + bytes((len(payload),))
        + payload
    )
    return STREAM_FRAME_MAGIC + body + bytes((sum(body) & 0xFF,))


def test_json_and_binary_frames_are_extracted_in_wire_order_across_partial_reads():
    client = GatewaySerialClient(BLERuntimeConfig(backend="gateway"))
    frame = _stream_frame(b"\x01\x02\x03")
    json_line = b'{"type":"status","ok":true}\n'
    second_json_line = b'{"type":"status_complete"}\n'

    client.buf.extend(json_line[:9])
    assert client._extract_item() is None
    assert bytes(client.buf) == json_line[:9]

    client.buf.extend(json_line[9:] + frame[:8])
    assert client._extract_item() == ("json", {"type": "status", "ok": True})
    assert client._extract_item() is None
    assert bytes(client.buf) == frame[:8]

    client.buf.extend(frame[8:] + second_json_line)
    item_type, parsed = client._extract_item()
    assert item_type == "stream_frame"
    assert parsed.sensor_id == 7
    assert parsed.gateway_timestamp_us == 123
    assert parsed.payload == b"\x01\x02\x03"
    assert client._extract_item() == ("json", {"type": "status_complete"})
    assert client.buf == b""
    assert client.stream_resync_events == 0
    assert client.stream_checksum_failures == 0


def test_resync_drop_count_is_bytes_present_before_next_valid_frame():
    client = GatewaySerialClient(BLERuntimeConfig(backend="gateway"))
    frame = _stream_frame(b"payload")
    orphaned = b"x" * 53
    client.buf.extend(orphaned + frame)

    item_type, parsed = client._extract_item()

    assert item_type == "stream_frame"
    assert parsed.payload == b"payload"
    assert client.stream_resync_drop_bytes == 53
    assert client.stream_resync_events == 1
    assert client.stream_checksum_failures == 0


def test_resync_dumps_bounded_serial_read_history(
    caplog: pytest.LogCaptureFixture,
):
    client = GatewaySerialClient(BLERuntimeConfig(backend="gateway"))
    client._serial_read_history = deque(maxlen=SERIAL_READ_HISTORY_SIZE)
    for index in range(SERIAL_READ_HISTORY_SIZE + 2):
        chunk = bytes((index, index + 1))
        client._serial_read_history.append(
            SerialReadCapture(
                monotonic_ns=1000 + index,
                read_len=len(chunk),
                buffer_len_before=index,
                buffer_len_after=index + len(chunk),
                chunk=chunk,
            )
        )
    client.buf.extend(b"bad" + STREAM_FRAME_MAGIC)

    gateway_ble_client.logger.addHandler(caplog.handler)
    try:
        client._drop_and_resync(3)
    finally:
        gateway_ble_client.logger.removeHandler(caplog.handler)

    history_records = [
        record
        for record in caplog.records
        if record.getMessage().startswith("GATEWAY SERIAL READ HISTORY")
    ]
    assert len(history_records) == 1
    message = history_records[0].getMessage()
    assert "trigger=resync" in message
    assert f"capture_count={SERIAL_READ_HISTORY_SIZE}" in message
    assert '"monotonic_ns":1002' in message
    assert '"chunk_hex":"0d0e"' in message


def test_checksum_failure_dumps_read_history_once(
    caplog: pytest.LogCaptureFixture,
):
    client = GatewaySerialClient(BLERuntimeConfig(backend="gateway"))
    valid_frame = _stream_frame(b"payload")
    corrupt_frame = valid_frame[:-1] + bytes((valid_frame[-1] ^ 0xFF,))
    client._serial_read_history.append(
        SerialReadCapture(
            monotonic_ns=999,
            read_len=len(corrupt_frame),
            buffer_len_before=0,
            buffer_len_after=len(corrupt_frame),
            chunk=corrupt_frame,
        )
    )
    client.buf.extend(corrupt_frame)

    gateway_ble_client.logger.addHandler(caplog.handler)
    try:
        assert client._extract_item() is None
    finally:
        gateway_ble_client.logger.removeHandler(caplog.handler)

    history_records = [
        record
        for record in caplog.records
        if record.getMessage().startswith("GATEWAY SERIAL READ HISTORY")
    ]
    assert len(history_records) == 1
    assert "trigger=checksum_failure" in history_records[0].getMessage()
