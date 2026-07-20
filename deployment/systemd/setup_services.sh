#!/usr/bin/env bash
set -e

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
user_systemd_dir="$HOME/.config/systemd/user"

mkdir -p "${user_systemd_dir}"

cp "${script_dir}/nexus_n3_master.service" "${user_systemd_dir}/"
cp "${script_dir}/nexus_n3_worker@.service" "${user_systemd_dir}/"

systemctl --user daemon-reload

systemctl --user enable --now nexus_n3_master.service

systemctl --user enable --now nexus_n3_worker@worker_A.service

systemctl --user status nexus_n3_master.service
systemctl --user status nexus_n3_worker@worker_A.service
