"""Optional IFMCU shell diagnostics for supported USB gateways."""

import re
import time
from datetime import datetime, timezone
from pathlib import Path


_ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_PROMPT = re.compile(r"ifsh:~\$\s*$")
_PAIR = re.compile(
    r"([A-Za-z_][A-Za-z0-9_]*)=(-?(?:0x[0-9a-fA-F]+|\d+))\b"
)


def _clean(raw):
    return _ANSI.sub("", raw.decode("utf-8", errors="replace")).replace(
        "\r", ""
    )


def _parse_stats(text):
    groups = {}
    for line in text.splitlines():
        matches = list(_PAIR.finditer(line))
        if not matches:
            continue

        group = (
            line[:matches[0].start()].strip().replace(" ", "_")
            or "general"
        )
        values = groups.setdefault(group, {})
        for match in matches:
            value = match.group(2)
            values[match.group(1)] = (
                int(value, 16) if "x" in value.lower() else int(value)
            )
    return groups


def query_bridge_stats(data_port, timeout_s=1.5, operation=None):
    """Return a JSON-safe result; never raise an optional query failure."""
    result = {
        "status": "unavailable",
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "data_port": str(data_port) if data_port else None,
        "shell_port": None,
        "command": "bridge stats",
        "raw_response": "",
    }
    started = time.monotonic()
    raw = bytearray()
    port = None

    try:
        # Only use the matching IFMCU shell, never arbitrary serial ports.
        path = Path(str(data_port or ""))
        if (
            path.parent != Path("/dev/serial/by-id")
            or not re.fullmatch(
                r"usb-ZEPHYR_IFMCU_CMSIS-DAP_[A-Za-z0-9]+-if01",
                path.name,
            )
        ):
            result["reason"] = "Active port is not a recognised IFMCU data interface"
            return result

        shell = path.with_name(path.name[:-4] + "if03")
        result["shell_port"] = str(shell)
        if not shell.exists():
            result["reason"] = "Matching IFMCU shell interface is absent"
            return result

        deadline = started + max(0.0, timeout_s)
        if operation is not None:
            deadline = min(deadline, operation.deadline)

        def remaining():
            if operation is not None and operation.cancelled.is_set():
                raise TimeoutError("Diagnostic operation cancelled")
            seconds = deadline - time.monotonic()
            if seconds <= 0:
                raise TimeoutError("IFMCU shell query timed out")
            return seconds

        import serial

        port = serial.Serial(
            port=str(shell),
            baudrate=115200,
            timeout=0.05,
            write_timeout=0.1,
            exclusive=True,
        )

        def send(command):
            port.write_timeout = min(0.1, remaining())
            if port.write(command) != len(command):
                raise RuntimeError("Incomplete IFMCU shell command write")

        def read_prompt():
            raw.clear()
            while True:
                port.timeout = min(0.05, remaining())
                raw.extend(port.read(256))
                if len(raw) > 16384:
                    raise RuntimeError("IFMCU shell response exceeds 16 KiB")
                text = _clean(raw)
                if _PROMPT.search(text):
                    return text

        # Synchronise with a fresh prompt before issuing the command.
        port.reset_input_buffer()
        send(b"\r")
        read_prompt()
        send(b"bridge stats\r")
        text = read_prompt()
        result["raw_response"] = text
        counters = _parse_stats(text)

        if (
            "uptime_ms" in counters.get("general", {})
            and "stopped" in counters.get("uart_rx", {})
        ):
            result["status"] = "ok"
            result["counters"] = counters
        elif re.search(
            r"unknown command|command not found|not a valid subcommand",
            text,
            re.IGNORECASE,
        ):
            result["status"] = "unsupported"
            result["reason"] = "Firmware does not support bridge stats"
        else:
            result["status"] = "invalid_response"
            result["reason"] = "Response lacks expected bridge statistics"

    except TimeoutError as exc:
        result["status"] = "timeout"
        result["reason"] = str(exc)
    except Exception as exc:
        result["status"] = "error"
        result["reason"] = f"{type(exc).__name__}: {exc}"
    finally:
        if raw and not result["raw_response"]:
            result["raw_response"] = _clean(raw)
        if port is not None:
            try:
                port.close()
            except Exception:
                pass
        result["duration_ms"] = round(
            (time.monotonic() - started) * 1000, 3
        )

    return result