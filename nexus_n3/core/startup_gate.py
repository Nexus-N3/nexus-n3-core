"""Startup gate helpers for strict stream bring-up."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class StartupGateSensorStats:
    """Tracks per-sensor startup metrics used by the strict gate."""

    address: str
    location: str | None
    expected_rate_hz: int | None
    stream_start_command_time: float | None = None
    first_packet_time: float | None = None
    startup_last_sensor_timestamp: int | None = None
    startup_first_wall_time: float | None = None
    startup_last_wall_time: float | None = None
    startup_packets_received: int = 0
    startup_gap_events: int = 0
    startup_estimated_dropped_packets: int = 0

    def reset_for_attempt(self, start_command_time: float | None = None) -> None:
        self.stream_start_command_time = start_command_time
        self.first_packet_time = None
        self.startup_last_sensor_timestamp = None
        self.startup_first_wall_time = None
        self.startup_last_wall_time = None
        self.startup_packets_received = 0
        self.startup_gap_events = 0
        self.startup_estimated_dropped_packets = 0

    def record_sample(self, sample, wall_time: float) -> None:

        
        timestamp = getattr(sample, "timestamp", None)
        sampling_rate = getattr(sample, "sampling_rate", None)

        if self.startup_packets_received < 20:
            print(
                "[STARTUP_GATE_SAMPLE]",
                self.address,
                "timestamp=", timestamp,
                "last=", self.startup_last_sensor_timestamp,
                "sampling_rate=", sampling_rate,
                "wall_time=", wall_time,
                flush=True,
            )

        if self.expected_rate_hz is None and sampling_rate:
            self.expected_rate_hz = int(sampling_rate)

        if self.first_packet_time is None:
            self.first_packet_time = wall_time
        if self.startup_first_wall_time is None:
            self.startup_first_wall_time = wall_time
        else:
            self._record_gap_if_needed(timestamp)

        self.startup_last_sensor_timestamp = timestamp
        self.startup_last_wall_time = wall_time
        self.startup_packets_received += 1

    def _record_gap_if_needed(self, timestamp: int | None) -> None:
        if timestamp is None or self.startup_last_sensor_timestamp is None:
            return

        expected_delta_us = self.expected_delta_us
        if expected_delta_us is None:
            return

        observed_delta_us = timestamp - self.startup_last_sensor_timestamp

        if observed_delta_us > int(expected_delta_us * 1.5):
            missing_packets = max(int(round(observed_delta_us / expected_delta_us)) - 1, 0)
            print(
                "[STARTUP_GATE_GAP]",
                self.address,
                "observed_delta_us=", observed_delta_us,
                "expected_delta_us=", expected_delta_us,
                "missing_packets=", missing_packets,
                "timestamp=", timestamp,
                "last=", self.startup_last_sensor_timestamp,
                flush=True,
            )

        if observed_delta_us <= int(expected_delta_us * 1.5):
            return

        missing_packets = max(int(round(observed_delta_us / expected_delta_us)) - 1, 0)
        if missing_packets <= 0:
            return

        self.startup_gap_events += 1
        self.startup_estimated_dropped_packets += missing_packets
        
    @property
    def expected_delta_us(self) -> float | None:
        if not self.expected_rate_hz:
            return None
        return 1_000_000.0 / float(self.expected_rate_hz)

    @property
    def startup_duration_seconds(self) -> float:
        if self.startup_first_wall_time is None or self.startup_last_wall_time is None:
            return 0.0
        return max(self.startup_last_wall_time - self.startup_first_wall_time, 0.0)

    @property
    def startup_observed_rate_hz(self) -> float:
        duration = self.startup_duration_seconds
        if duration <= 0:
            return 0.0
        return self.startup_packets_received / duration

    @property
    def time_to_first_packet_ms(self) -> float | None:
        if self.stream_start_command_time is None or self.first_packet_time is None:
            return None
        return max((self.first_packet_time - self.stream_start_command_time) * 1000.0, 0.0)

    def as_status_payload(self) -> dict:
        return {
            "address": self.address,
            "location": self.location,
            "expected_rate_hz": self.expected_rate_hz,
            "startup_packets": self.startup_packets_received,
            "startup_rate_hz": round(self.startup_observed_rate_hz, 3),
            "time_to_first_packet_ms": (
                None if self.time_to_first_packet_ms is None else round(self.time_to_first_packet_ms, 3)
            ),
            "startup_gap_events": self.startup_gap_events,
            "startup_estimated_dropped_packets": self.startup_estimated_dropped_packets,
        }
