"""Device discovery result types."""

from typing import NamedTuple



class DevicesValid(NamedTuple):
    """Discovery validation results."""
    valid: bool
    missing: list[str]
    found: int
