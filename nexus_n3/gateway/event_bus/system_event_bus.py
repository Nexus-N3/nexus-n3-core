"""Threaded event bus for system-wide events."""

from nexus_n3.logger.logger import get_module_logger
import time
logger = get_module_logger("System Event Bus")

from queue import Queue
import threading
from copy import deepcopy


def _enrich_event(event: dict, deployment_context: dict[str, str | None]) -> dict:
    """Inject deployment context into a system event without overwriting explicit values."""

    enriched = deepcopy(event)
    payload = enriched.get("payload")

    customer_id = deployment_context.get("customer_id")
    site_id = deployment_context.get("site_id")
    site_name = deployment_context.get("site_name")

    if customer_id and not enriched.get("customer_id"):
        enriched["customer_id"] = customer_id
    if site_id and not enriched.get("site_id"):
        enriched["site_id"] = site_id
    if site_name and not enriched.get("site"):
        enriched["site"] = site_name

    if isinstance(payload, dict):
        if customer_id and not payload.get("customer_id"):
            payload["customer_id"] = customer_id
        if site_id and not payload.get("site_id"):
            payload["site_id"] = site_id
        if site_name and not payload.get("site"):
            payload["site"] = site_name

    return enriched

class SystemEventBus:
    """Simple pub-sub queue for system events."""
    def __init__(self, deployment_context: dict[str, str | None] | None = None):
        """Initialize the event bus and start the dispatcher thread."""
        self._subs = []
        self._queue = Queue()
        self._deployment_context = deployment_context or {}

        self._worker = threading.Thread(
            target=self._dispatch_loop,
            daemon=True
        )
        self._worker.start()

    def subscribe(self, cb):
        """
        Register a subscriber callback.

        Args:
            cb: Callable that accepts an event dict.
        """
        self._subs.append(cb)

    def emit(self, event: dict):
        """
        Emit an event to subscribers asynchronously.

        Args:
            event: Event dictionary containing type and payload.
        """
        enriched_event = _enrich_event(event, self._deployment_context)
        logger.info(f"{enriched_event} at {time.time()}")
        self._queue.put(enriched_event)

    def _dispatch_loop(self):
        """Internal dispatcher loop that delivers events to subscribers."""
        while True:
            event = self._queue.get()
            for cb in self._subs:
                try:
                    cb(event)
                except Exception as e:
                    logger.error(f"Event subscriber error: {e}")
