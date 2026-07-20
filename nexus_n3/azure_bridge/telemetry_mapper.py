"""Helpers for mapping local Nexus N3 Core events to Azure telemetry."""

from __future__ import annotations

from datetime import datetime, timezone
import uuid


def utc_now_iso() -> str:
    """Return a compact UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def map_event_for_cloud(
    event: dict,
    *,
    device_id: str,
    customer_id: str | None = None,
    site_id: str | None = None,
    site: str | None = None,
    correlation_id: str | None = None,
) -> dict:
    """Preserve the event shape and enrich it for cloud telemetry."""
    telemetry = dict(event)
    telemetry["device_id"] = device_id
    telemetry["timestamp"] = utc_now_iso()
    if customer_id and not telemetry.get("customer_id"):
        telemetry["customer_id"] = customer_id
    if site_id and not telemetry.get("site_id"):
        telemetry["site_id"] = site_id
    if site and not telemetry.get("site"):
        telemetry["site"] = site
    if correlation_id:
        telemetry["correlation_id"] = correlation_id
    return telemetry


def build_method_response_payload(*, status: int, message: str, correlation_id: str | None = None, extra: dict | None = None) -> dict:
    """Build a consistent direct-method response body."""
    payload = {
        "status": status,
        "message": message,
    }
    if correlation_id:
        payload["correlation_id"] = correlation_id
    if extra:
        payload.update(extra)
    return payload


def new_correlation_id() -> str:
    """Generate a correlation identifier for cloud-initiated actions."""
    return str(uuid.uuid4())
