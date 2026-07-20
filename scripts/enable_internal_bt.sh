#!/usr/bin/env bash
set -euo pipefail

if [ -f /boot/firmware/config.txt ]; then
  CONFIG_FILE=/boot/firmware/config.txt
else
  CONFIG_FILE=/boot/config.txt
fi

echo "Using boot config: $CONFIG_FILE"

echo "== Comment out dtoverlay=disable-bt =="
sudo sed -i 's/^\s*dtoverlay=disable-bt/#dtoverlay=disable-bt/' "$CONFIG_FILE"

echo "== Enable hciuart service =="
sudo systemctl enable hciuart 2>/dev/null || true

echo "== Set BlueZ AutoEnable=true =="
if grep -q '^AutoEnable=' /etc/bluetooth/main.conf; then
  sudo sed -i 's/^AutoEnable=.*/AutoEnable=true/' /etc/bluetooth/main.conf
else
  sudo sed -i '/^\[Policy\]/a AutoEnable=true' /etc/bluetooth/main.conf
fi

echo
echo "Internal Bluetooth re-enabled in boot config."
echo "Reboot required:"
echo "  sudo reboot"
