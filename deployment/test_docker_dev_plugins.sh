#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
compose_dir="${repo_root}/deployment/docker"
compose_file="${compose_dir}/docker-compose.dev.yml"

host_plugin_root="/opt/nexus-n3-plugins"
bundle_path=""
keep_running=0
skip_build=0
service_name="nexus-n3-core"
project_name="${COMPOSE_PROJECT_NAME:-nexus_n3_dev_test}"

usage() {
  cat <<EOF
Usage: bash deployment/test_docker_dev_plugins.sh [options]

Starts the dev Docker compose stack, waits for the admin surface, inspects the
live plugin inventory, and optionally installs a plugin bundle while the server
is running to verify that the runtime sees the new plugin without a restart.

Options:
  --bundle PATH       Install this .rsnxplugin bundle while the stack is running.
  --keep-running      Leave the compose stack running after the test completes.
  --skip-build        Use docker compose up without --build.
  -h, --help          Show this help.

Environment:
  COMPOSE_PROJECT_NAME
      Optional docker compose project name.
      Default: ${project_name}

Examples:
  bash deployment/test_docker_dev_plugins.sh
  bash deployment/test_docker_dev_plugins.sh --bundle /path/to/plugin.rsnxplugin
EOF
}

log() {
  printf '[test-docker-dev] %s\n' "$*"
}

cleanup() {
  if [[ "${keep_running}" -eq 0 ]]; then
    log "Stopping docker compose stack"
    (
      cd "${compose_dir}"
      COMPOSE_PROJECT_NAME="${project_name}" \
      docker compose -f "${compose_file}" down >/dev/null 2>&1 || true
    )
  fi
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

bundle_manifest_field() {
  local bundle="$1"
  local field="$2"
  python3 - "$bundle" "$field" <<'PY'
import json
import sys
import zipfile

bundle_path = sys.argv[1]
field = sys.argv[2]
with zipfile.ZipFile(bundle_path) as archive:
    manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
value = manifest.get(field, "")
if isinstance(value, (dict, list)):
    print(json.dumps(value))
else:
    print(value)
PY
}

capture_inventory() {
  local container_id="$1"
  docker exec "${container_id}" python3 - <<'PY'
import json
from nexus_n3.plugins.runtime.discovery import (
    get_installed_plugin_inventory,
    get_supported_algorithms,
    get_supported_sensors,
)

payload = {
    "inventory": get_installed_plugin_inventory(),
    "supported_sensors": get_supported_sensors(),
    "supported_algorithms": get_supported_algorithms(),
}
print(json.dumps(payload, indent=2, sort_keys=True))
PY
}

wait_for_admin() {
  local deadline
  deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if curl -fsS "http://127.0.0.1:9000/capabilities" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bundle)
      bundle_path="$2"
      shift 2
      ;;
    --keep-running)
      keep_running=1
      shift
      ;;
    --skip-build)
      skip_build=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

require_cmd docker
require_cmd curl
require_cmd python3

if [[ -n "${bundle_path}" && ! -f "${bundle_path}" ]]; then
  echo "Bundle not found: ${bundle_path}" >&2
  exit 1
fi

mkdir -p "${host_plugin_root}"
trap cleanup EXIT

log "Using host plugin root: ${host_plugin_root}"
log "Starting docker compose dev stack"
compose_args=(docker compose -f "${compose_file}" up -d)
if [[ "${skip_build}" -eq 0 ]]; then
  compose_args+=(--build)
fi
(
  cd "${compose_dir}"
  COMPOSE_PROJECT_NAME="${project_name}" \
  "${compose_args[@]}"
)

container_id="$(
  cd "${compose_dir}" && \
  COMPOSE_PROJECT_NAME="${project_name}" \
  docker compose -f "${compose_file}" ps -q "${service_name}"
)"

if [[ -z "${container_id}" ]]; then
  echo "Failed to resolve container id for service ${service_name}" >&2
  exit 1
fi

log "Waiting for admin surface on http://127.0.0.1:9000/capabilities"
if ! wait_for_admin; then
  log "Admin surface did not become ready in time"
  docker logs "${container_id}" || true
  exit 1
fi

log "Admin surface is reachable"

if docker logs "${container_id}" 2>&1 | grep -q "\[PLUGINS\]"; then
  log "Startup plugin summary detected in container logs"
else
  log "Startup plugin summary not found yet; continuing"
fi

before_snapshot="$(mktemp)"
after_snapshot=""

capture_inventory "${container_id}" | tee "${before_snapshot}"

if [[ -n "${bundle_path}" ]]; then
  bundle_name="$(basename "${bundle_path}")"
  plugin_id="$(bundle_manifest_field "${bundle_path}" "plugin_id")"
  plugin_version="$(bundle_manifest_field "${bundle_path}" "version")"

  if python3 - "${before_snapshot}" "${plugin_id}" "${plugin_version}" <<'PY'
import json
import sys

snapshot_path, plugin_id, version = sys.argv[1:4]
with open(snapshot_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
plugins = payload["inventory"].get("sensor_plugins", []) + payload["inventory"].get("algorithm_plugins", [])
for item in plugins:
    if item.get("plugin_id") == plugin_id and item.get("version") == version:
        raise SystemExit(0)
raise SystemExit(1)
PY
  then
    log "Bundle ${plugin_id} ${plugin_version} is already present in the live inventory"
  else
    log "Installing ${bundle_name} while the server is running"
  fi

  docker cp "${bundle_path}" "${container_id}:/tmp/${bundle_name}"
  docker exec "${container_id}" python3 -m nexus_n3.plugins install \
    "/tmp/${bundle_name}" \
    --plugin-root /opt/nexus-n3-plugins

  after_snapshot="$(mktemp)"
  capture_inventory "${container_id}" | tee "${after_snapshot}"

  python3 - "${after_snapshot}" "${plugin_id}" "${plugin_version}" <<'PY'
import json
import sys

snapshot_path, plugin_id, version = sys.argv[1:4]
with open(snapshot_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
plugins = payload["inventory"].get("sensor_plugins", []) + payload["inventory"].get("algorithm_plugins", [])
for item in plugins:
    if item.get("plugin_id") == plugin_id and item.get("version") == version:
        print(f"Verified live inventory contains {plugin_id} {version}")
        raise SystemExit(0)
raise SystemExit(f"Installed bundle {plugin_id} {version} did not appear in live inventory")
PY
else
  log "No --bundle supplied; tested startup and live inventory only"
  log "To test point 4, rerun with --bundle /path/to/plugin.rsnxplugin"
fi

log "Test completed successfully"
if [[ "${keep_running}" -eq 1 ]]; then
  log "Leaving stack running because --keep-running was requested"
fi
