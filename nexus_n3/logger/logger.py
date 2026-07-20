"""Module-level logger configuration with rotating file output."""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys

# ---------------- LOG DIRECTORY ----------------
LOG_DIR = Path.cwd() / "nexus_n3_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
print(f"Logs directory: {LOG_DIR}")

# ---------------- LOG CONFIGURATION ----------------
LOG_LEVEL = logging.DEBUG
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s"
MAX_BYTES = 10 * 1024 * 1024  # 10 MB per file
BACKUP_COUNT = 5  # keep last 5 log files


def get_module_logger(module_name: str) -> logging.Logger:
    """
    Create and return a module-specific logger.

    Args:
        module_name: Name of the module, used to name the log file.

    Returns:
        Configured logger instance.

    Notes:
        - Logs are written to `nexus_n3_logs/<module>.log` with rotation.
        - Console output is emitted only when `extra={'console': True}` is set.
    """
    logger = logging.getLogger(module_name)

    # Prevent duplicate handlers if already configured
    if logger.handlers:
        return logger

    log_file = LOG_DIR / f"{module_name}.log"

    # --- File handler ---
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    file_handler.setLevel(LOG_LEVEL)
    logger.addHandler(file_handler)

    # --- Console handler (conditional by 'console' flag) ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    console_handler.setLevel(logging.DEBUG)

    class ConsoleFilter(logging.Filter):
        """Filter log records to only output those with extra={'console': True}."""
        def filter(self, record):
            """Return True when the record requests console output."""
            return getattr(record, 'console', False)

    console_handler.addFilter(ConsoleFilter())
    logger.addHandler(console_handler)

    logger.setLevel(LOG_LEVEL)
    logger.propagate = False  # prevent propagation to root logger

    return logger
