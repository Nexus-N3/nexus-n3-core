# nexus_n3.logger

## Overview
`nexus_n3.logger` provides a rotating file logger per module, with optional
console output controlled via `extra={"console": True}`.

## Key Functions
- `get_module_logger(module_name)`
  - Creates `nexus_n3_logs/<module_name>.log`
  - Rotates at 10 MB with up to 5 backups
  - Optional console logging via `extra={"console": True}`

## Key Files
- `nexus_n3.logger/logger.py`
