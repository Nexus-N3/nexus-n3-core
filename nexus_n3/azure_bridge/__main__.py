"""CLI entrypoint for the Azure bridge."""

from __future__ import annotations

from .bridge import AzureBridgeService
from .config import AzureBridgeConfig


def main() -> None:
    config = AzureBridgeConfig.from_env()
    AzureBridgeService(config).start()


if __name__ == "__main__":
    main()
