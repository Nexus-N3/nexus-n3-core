#!/usr/bin/env bash
set -euo pipefail

RS_USER="${RS_USER:-nexusn3}"
USB_LABEL="${USB_LABEL:-RSNEXUSDATA}"
USB_LABEL_DEV="${USB_LABEL_DEV:-${USB_LABEL// /\\x20}}"
DEV_PATH="${DEV_PATH:-/dev/disk/by-label/${USB_LABEL_DEV}}"
MAIN_MOUNT="${MAIN_MOUNT:-/exports/nexus_n3_data}"
GUI_MOUNT="${GUI_MOUNT:-/home/${RS_USER}/USB}"
USB_FS_TYPE="${USB_FS_TYPE:-exfat}"
USB_FMASK="${USB_FMASK:-0002}"
USB_DMASK="${USB_DMASK:-0002}"

if ! id -u "${RS_USER}" >/dev/null 2>&1; then
  echo "ERROR: user '${RS_USER}' does not exist" >&2
  exit 1
fi

RS_UID="$(id -u "${RS_USER}")"
RS_GID="$(id -g "${RS_USER}")"

if [[ ! -e "${DEV_PATH}" ]]; then
  echo "ERROR: device not found at ${DEV_PATH}" >&2
  exit 1
fi

mkdir -p "${MAIN_MOUNT}" "${GUI_MOUNT}"

if ! mountpoint -q "${MAIN_MOUNT}"; then
  mount -t "${USB_FS_TYPE}" \
    -o "uid=${RS_UID},gid=${RS_GID},fmask=${USB_FMASK},dmask=${USB_DMASK}" \
    "${DEV_PATH}" "${MAIN_MOUNT}"
fi

if ! mountpoint -q "${GUI_MOUNT}"; then
  mount --bind "${MAIN_MOUNT}" "${GUI_MOUNT}"
fi

sync
mountpoint -q "${MAIN_MOUNT}" && echo "OK: ${MAIN_MOUNT} mounted"
mountpoint -q "${GUI_MOUNT}" && echo "OK: ${GUI_MOUNT} mounted (bind)"
