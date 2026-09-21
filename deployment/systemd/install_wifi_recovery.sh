#!/usr/bin/env bash
set -euo pipefail

if [ "${EUID}" -ne 0 ]; then
    echo "Run this installer as root: sudo $0 [service-user] [country-code]" >&2
    exit 2
fi

service_user="${1:-${SUDO_USER:-}}"
country_code="${2:-EE}"

if [[ ! "${service_user}" =~ ^[a-z_][a-z0-9_-]*[$]?$ ]]; then
    echo "A valid Nexus service user is required." >&2
    exit 2
fi
if ! id "${service_user}" >/dev/null 2>&1; then
    echo "Nexus service user ${service_user} does not exist." >&2
    exit 2
fi
if [[ ! "${country_code}" =~ ^[A-Za-z]{2}$ ]]; then
    echo "The regulatory country code must contain exactly two letters." >&2
    exit 2
fi
country_code="${country_code^^}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
unit_source="${script_dir}/nexus-n3-wifi-recovery.service"
sudoers_source="${script_dir}/nexus-n3-wifi-recovery.sudoers"

install -o root -g root -m 0644 \
    "${unit_source}" \
    /etc/systemd/system/nexus-n3-wifi-recovery.service
install -d -o root -g root -m 0755 /etc/nexus-n3
printf 'NEXUS_WIFI_REGULATORY_DOMAIN=%s\n' "${country_code}" \
    > /etc/nexus-n3/wifi-recovery.env
chown root:root /etc/nexus-n3/wifi-recovery.env
chmod 0644 /etc/nexus-n3/wifi-recovery.env

escaped_user="${service_user//&/\\&}"
sed "s/@NEXUS_SERVICE_USER@/${escaped_user}/g" "${sudoers_source}" \
    > /etc/sudoers.d/nexus-n3-wifi-recovery
chown root:root /etc/sudoers.d/nexus-n3-wifi-recovery
chmod 0440 /etc/sudoers.d/nexus-n3-wifi-recovery
visudo -cf /etc/sudoers.d/nexus-n3-wifi-recovery

# Remove the superseded PolicyKit rule so it cannot open an authentication
# agent for this operation on hosts where it was installed previously.
rm -f /etc/polkit-1/rules.d/49-nexus-n3-wifi-recovery.rules

systemctl daemon-reload
echo "Installed Wi-Fi recovery permission for ${service_user}."
