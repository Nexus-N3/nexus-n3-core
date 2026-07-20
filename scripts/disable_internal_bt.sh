#!/usr/bin/env bash
set -euo pipefail

if [ -f /boot/firmware/config.txt ]; then
  CONFIG_FILE=/boot/firmware/config.txt
else
  CONFIG_FILE=/boot/config.txt
fi

echo "Using boot config: $CONFIG_FILE"

echo "== Disable hciuart service =="
sudo systemctl disable --now hciuart 2>/dev/null || true

echo "== Add dtoverlay=disable-bt if missing =="
if grep -qE '^\s*#?\s*dtoverlay=disable-bt' "$CONFIG_FILE"; then
  sudo sed -i 's/^\s*#\s*dtoverlay=disable-bt/dtoverlay=disable-bt/' "$CONFIG_FILE"
else
  echo 'dtoverlay=disable-bt' | sudo tee -a "$CONFIG_FILE" >/dev/null
fi

echo "== Set BlueZ AutoEnable=false =="
if grep -q '^AutoEnable=' /etc/bluetooth/main.conf; then
  sudo sed -i 's/^AutoEnable=.*/AutoEnable=false/' /etc/bluetooth/main.conf
else
  sudo sed -i '/^\[Policy\]/a AutoEnable=false' /etc/bluetooth/main.conf
fi

echo
echo "Internal Bluetooth disabled in boot config."
echo "Reboot required:"
echo "  sudo reboot"
