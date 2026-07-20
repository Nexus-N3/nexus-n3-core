#!/usr/bin/env bash
set -euo pipefail

RS_USER="${RS_USER:-nexusn3}"
MAIN_MOUNT="${MAIN_MOUNT:-/exports/nexus_n3_data}"
GUI_MOUNT="${GUI_MOUNT:-/home/${RS_USER}/USB}"

sync

if mountpoint -q "${GUI_MOUNT}"; then
  umount "${GUI_MOUNT}"
fi

if mountpoint -q "${MAIN_MOUNT}"; then
  umount "${MAIN_MOUNT}"
fi

sync

if mountpoint -q "${MAIN_MOUNT}" || mountpoint -q "${GUI_MOUNT}"; then
  echo "ERROR: one or more mount points are still mounted" >&2
  exit 1
fi

echo "Finalize complete: safe to unplug."
