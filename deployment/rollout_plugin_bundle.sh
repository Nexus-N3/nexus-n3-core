#!/usr/bin/env bash
set -euo pipefail

inventory="deployment/ansible/inventory.ini"
hosts="master"
bundle=""
plugin_root="/opt/nexus-n3-plugins"
remote_dir="/tmp/nexus-n3-plugin-rollout"
venv_python="/opt/nexus-n3-core/venv/bin/python"
become_args=("--become")
system_site_packages=0

usage() {
  cat <<'EOF'
Usage:
  bash deployment/rollout_plugin_bundle.sh --bundle /path/to/plugin.rsnxplugin [options]

The bundle path may point to a local build output, a mounted USB drive, or
another directory populated by a future admin-app upload flow.

Options:
  --bundle PATH          Built .rsnxplugin bundle to deploy
  --hosts PATTERN        Ansible host pattern or group (default: master)
  --inventory PATH       Ansible inventory path
  --plugin-root PATH     Target plugin root (default: /opt/nexus-n3-plugins)
  --remote-dir PATH      Temporary remote staging dir
  --venv-python PATH     Target nexus-n3-core venv python
  --no-become            Do not pass --become to ansible
  --system-site-packages Pass --system-site-packages to the plugin installer
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bundle)
      bundle="$2"
      shift 2
      ;;
    --hosts)
      hosts="$2"
      shift 2
      ;;
    --inventory)
      inventory="$2"
      shift 2
      ;;
    --plugin-root)
      plugin_root="$2"
      shift 2
      ;;
    --remote-dir)
      remote_dir="$2"
      shift 2
      ;;
    --venv-python)
      venv_python="$2"
      shift 2
      ;;
    --no-become)
      become_args=()
      shift
      ;;
    --system-site-packages)
      system_site_packages=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "${bundle}" ]]; then
  echo "--bundle is required" >&2
  usage
  exit 2
fi

if [[ ! -f "${bundle}" ]]; then
  echo "Bundle not found: ${bundle}" >&2
  exit 1
fi

bundle_name="$(basename "${bundle}")"

manifest_json="$(python3 - "${bundle}" <<'PY'
import json
import sys
import zipfile

with zipfile.ZipFile(sys.argv[1]) as archive:
    print(json.dumps(json.loads(archive.read("manifest.json").decode("utf-8"))))
PY
)"

plugin_id="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["plugin_id"])' "${manifest_json}")"
version="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["version"])' "${manifest_json}")"
install_manifest="${plugin_root}/installed/${plugin_id}/${version}/manifest.json"

ansible "${hosts}" -i "${inventory}" "${become_args[@]}" \
  -m file -a "path=${remote_dir} state=directory mode=0755"

ansible "${hosts}" -i "${inventory}" "${become_args[@]}" \
  -m copy -a "src=${bundle} dest=${remote_dir}/${bundle_name} mode=0644"

installer_cmd="${venv_python} -m nexus_n3.plugins install ${remote_dir}/${bundle_name} --plugin-root ${plugin_root}"
if [[ "${system_site_packages}" -eq 1 ]]; then
  installer_cmd="${installer_cmd} --system-site-packages"
fi

ansible "${hosts}" -i "${inventory}" "${become_args[@]}" \
  -m shell -a "if [ -f '${install_manifest}' ]; then exit 0; fi; ${installer_cmd}"
