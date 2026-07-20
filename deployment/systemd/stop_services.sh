#!/usr/bin/env bash
set -e

echo "[cluster] stopping Nexus N3 Core services..."

# Stop master
systemctl --user stop nexus_n3_master.service

# Stop standalone
systemctl --user stop nexus_n3_standalone.service

# Stop all worker instances
workers=$(systemctl --user list-units --type=service --no-legend | grep 'nexus_n3_worker@' | awk '{print $1}')
for w in $workers; do
    systemctl --user stop "$w"
done

# Disable master
systemctl --user disable nexus_n3_master.service

# disable standalone
systemctl --user disable nexus_n3_standalone.service

# Disable all worker instances
for w in $workers; do
    systemctl --user disable "$w"
done

# Reload systemd to clean up dead links
systemctl --user daemon-reload

echo "[cluster] Nexus N3 Core services stopped and disabled."
