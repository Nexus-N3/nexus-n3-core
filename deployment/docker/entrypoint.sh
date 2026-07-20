#!/usr/bin/env bash
set -euo pipefail

cd /app/nexus-n3-core

export NEXUS_N3_ENV_FILE="${NEXUS_N3_ENV_FILE:-/app/nexus-n3-core/config/runtime.env}"

if [[ -n "${NEXUS_N3_PLUGIN_BUNDLE_DIR:-}" && -d "${NEXUS_N3_PLUGIN_BUNDLE_DIR}" ]]; then
  shopt -s nullglob
  for bundle in "${NEXUS_N3_PLUGIN_BUNDLE_DIR}"/*.rsnxplugin; do
    if python - "${bundle}" "${NEXUS_N3_PLUGIN_ROOT}" <<'PY'
import json
import sys
import zipfile
from pathlib import Path

bundle_path = Path(sys.argv[1])
plugin_root = Path(sys.argv[2])
with zipfile.ZipFile(bundle_path) as archive:
    manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
install_manifest = plugin_root / "installed" / manifest["plugin_id"] / manifest["version"] / "manifest.json"
raise SystemExit(0 if install_manifest.exists() else 1)
PY
    then
      continue
    fi

    python -m nexus_n3.plugins install \
      "${bundle}" \
      --plugin-root "${NEXUS_N3_PLUGIN_ROOT}"
  done
  shopt -u nullglob
fi

if [[ -n "${NEXUS_N3_PLUGIN_CATALOG_ROOT:-}" ]]; then
  exec python nexus_n3_server.py \
    --plugin-root "${NEXUS_N3_PLUGIN_ROOT}" \
    --plugin-use-system-site-packages \
    --prepare-plugin-catalog-root "${NEXUS_N3_PLUGIN_CATALOG_ROOT}" \
    --plugin-tooling-root /app/nexus-n3-plugin-tooling \
    "${@}"
else
  exec python nexus_n3_server.py "${@}"
fi
