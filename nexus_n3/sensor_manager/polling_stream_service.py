"""Polling-based fallback stream service."""

import asyncio


class PollingStreamService:
    """Fallback for sensors that support request_sample but not start_stream."""

    def __init__(self, loop):
        self.loop = loop
        self._stream_tasks = {}
        self._stream_stop_events = {}

    async def start(self, sensor, adapter):
        if sensor in self._stream_tasks and not self._stream_tasks[sensor].done():
            return
        stop_event = asyncio.Event()
        self._stream_stop_events[sensor] = stop_event
        self._stream_tasks[sensor] = self.loop.create_task(
            self._poll_sensor(sensor, adapter, stop_event)
        )

    async def stop(self, sensor):
        if sensor in self._stream_stop_events:
            self._stream_stop_events[sensor].set()
        if sensor in self._stream_tasks:
            self._stream_tasks[sensor].cancel()

    async def stop_all(self):
        for sensor in list(self._stream_tasks.keys()):
            await self.stop(sensor)

    async def _poll_sensor(self, sensor, adapter, stop_event):
        rate = getattr(sensor, "attributes", {}).get("SAMPLING_RATE")
        delay = 1 / rate if rate else 0.05
        while not stop_event.is_set():
            await sensor.request_sample(adapter)
            await asyncio.sleep(delay)
