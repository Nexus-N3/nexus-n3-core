#!/usr/bin/env bash
set -euo pipefail

# Nexus N3 Core Standalone Health Check
# - Prints a concise PASS/FAIL summary at the end
# - Still emits detailed diagnostics for troubleshooting
#
# Exit codes:
#   0 = PASS (no critical failures)
#   1 = FAIL (one or more critical checks failed)

print_header() {
  echo
  echo "== $1 =="
}

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0
FAIL_REASONS=()

pass() {
  ((PASS_COUNT+=1))
  echo "[PASS] $1"
}

warn() {
  ((WARN_COUNT+=1))
  echo "[WARN] $1"
}

fail() {
  ((FAIL_COUNT+=1))
  echo "[FAIL] $1"
  FAIL_REASONS+=("$1")
}

has_cmd() {
  command -v "$1" >/dev/null 2>&1
}

# Best-effort runner: does not fail the script
run_best_effort() {
  local desc="$1"; shift
  print_header "$desc"
  "$@" || true
}

# Critical check runner: increments fail counter if command fails
check_cmd() {
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then
    pass "$desc"
    return 0
  else
    fail "$desc"
    return 1
  fi
}

# Non-critical check runner: increments warn counter if command fails
check_cmd_warn() {
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then
    pass "$desc"
    return 0
  else
    warn "$desc"
    return 1
  fi
}

echo "Nexus N3 Core Standalone Health Check"
echo "--------------------------------"

print_header "Host"
hostname || true
date || true

run_best_effort "Network Interfaces" ip -br a

# --- Determine whether AP mode is expected/enabled ---
# Heuristic: if hostapd is active or wlan0 has an IP in 192.168.50.0/24, treat AP as "enabled".
AP_ENABLED="unknown"
WLAN0_HAS_AP_IP="no"
if ip -4 addr show wlan0 2>/dev/null | grep -qE "inet 192\.168\.50\."; then
  WLAN0_HAS_AP_IP="yes"
fi

HOSTAPD_ACTIVE="no"
if systemctl is-active --quiet hostapd 2>/dev/null; then
  HOSTAPD_ACTIVE="yes"
fi

if [[ "$HOSTAPD_ACTIVE" == "yes" || "$WLAN0_HAS_AP_IP" == "yes" ]]; then
  AP_ENABLED="yes"
else
  AP_ENABLED="no"
fi

print_header "AP Mode Detection"
echo "hostapd active: $HOSTAPD_ACTIVE"
echo "wlan0 has 192.168.50.x IP: $WLAN0_HAS_AP_IP"
echo "AP enabled (heuristic): $AP_ENABLED"

run_best_effort "AP IP (wlan0)" ip addr show wlan0

# --- Services ---
print_header "Services"
systemctl --no-pager --full status nexus-n3 || true
systemctl --no-pager --full status hostapd || true
systemctl --no-pager --full status dnsmasq || true
systemctl --no-pager --full status nexusn3-ap-ip.service || true

# Critical in all modes: nexus-n3 service should be running
check_cmd "nexus-n3 service is active" systemctl is-active --quiet nexus-n3

# AP services are only required if AP mode is enabled
if [[ "$AP_ENABLED" == "yes" ]]; then
  check_cmd "hostapd service is active (AP enabled)" systemctl is-active --quiet hostapd
  check_cmd "dnsmasq service is active (AP enabled)" systemctl is-active --quiet dnsmasq
  # nexusn3-ap-ip.service might not exist on some images; treat as warning if absent/inactive
  if systemctl list-unit-files | grep -q "^nexusn3-ap-ip\.service"; then
    check_cmd_warn "nexusn3-ap-ip.service is active (AP enabled)" systemctl is-active --quiet nexusn3-ap-ip.service
  else
    warn "nexusn3-ap-ip.service not installed (AP enabled) — ensure AP IP configuration is handled elsewhere"
  fi
else
  pass "AP services not required (AP disabled)"
fi

# --- Listening ports (critical) ---
print_header "Listening Ports"
# Always: SSH and Admin UI should be reachable locally (at least bound)
# Note: SSH may not be required in some environments, but for operational/service management it's expected.
SS_OUT="$(ss -lntp 2>/dev/null || true)"
echo "$SS_OUT" | grep -E ":(22|9000)\b" || true

if echo "$SS_OUT" | grep -qE ":9000\b"; then
  pass "Admin UI port 9000 is listening"
else
  fail "Admin UI port 9000 is not listening"
fi

if echo "$SS_OUT" | grep -qE ":22\b"; then
  pass "SSH port 22 is listening"
else
  warn "SSH port 22 is not listening (may be intentional in some deployments)"
fi

# AP DHCP port check only if AP enabled
if [[ "$AP_ENABLED" == "yes" ]]; then
  SSU_OUT="$(ss -lunp 2>/dev/null || true)"
  echo "$SSU_OUT" | grep -E ":67\b" || true
  if echo "$SSU_OUT" | grep -qE ":67\b"; then
    pass "DHCP port 67/udp is listening (AP enabled)"
  else
    fail "DHCP port 67/udp is not listening (AP enabled)"
  fi
else
  pass "DHCP port 67/udp not required (AP disabled)"
fi

# --- Firewall ---
print_header "Firewall (ufw)"
if has_cmd ufw; then
  ufw status || true
  pass "ufw command available"
else
  warn "ufw not installed (firewall status not checked)"
fi

# --- USB / Storage (optional but important if SSD is part of the deployment profile) ---
# We do not know if SSD is expected. Treat missing mount as WARN, but validate directory permissions if present.
print_header "USB Mount / Storage Paths"
MOUNT_OUT="$(mount 2>/dev/null || true)"
echo "$MOUNT_OUT" | grep nexus_n3_data || true

if echo "$MOUNT_OUT" | grep -q "nexus_n3_data"; then
  pass "nexus_n3_data mount present"
else
  warn "nexus_n3_data mount not present (SSD may be absent or not mounted)"
fi

ls -ld /exports /exports/nexus_n3_data /exports/nexus_n3_data/nexus_n3_outputs 2>/dev/null || true
if [[ -d /exports/nexus_n3_data/nexus_n3_outputs ]]; then
  if [[ -w /exports/nexus_n3_data/nexus_n3_outputs ]]; then
    pass "outputs directory is writable: /exports/nexus_n3_data/nexus_n3_outputs"
  else
    fail "outputs directory is not writable: /exports/nexus_n3_data/nexus_n3_outputs"
  fi
else
  warn "outputs directory missing: /exports/nexus_n3_data/nexus_n3_outputs (may be unconfigured for this profile)"
fi

# --- USB hotplug script sanity (optional) ---
print_header "USB Hotplug Script"
if [[ -f /usr/local/bin/nexusn3-hotplug.sh ]]; then
  grep -E "uid=|gid=" /usr/local/bin/nexusn3-hotplug.sh || true
  pass "hotplug script present: /usr/local/bin/nexusn3-hotplug.sh"
else
  warn "hotplug script not found: /usr/local/bin/nexusn3-hotplug.sh (may be unconfigured for this profile)"
fi

# --- DHCP leases (informational) ---
print_header "DHCP Leases"
if [[ -f /var/lib/misc/dnsmasq.leases ]]; then
  cat /var/lib/misc/dnsmasq.leases || true
  pass "dnsmasq leases file present"
else
  warn "dnsmasq leases file not found (AP may be disabled or dnsmasq not configured)"
fi

# --- Admin UI probe (critical) ---
print_header "Admin UI Probe"
ADMIN_OK="no"

if has_cmd curl; then
  if curl -fsS http://127.0.0.1:9000/ >/dev/null; then
    ADMIN_OK="yes"
  fi
else
  # Python fallback
  if has_cmd python3; then
    if python3 - <<'PY'
import sys
from urllib.request import urlopen
try:
    with urlopen("http://127.0.0.1:9000/", timeout=3) as resp:
        sys.exit(0 if resp.status == 200 else 1)
except Exception:
    sys.exit(1)
PY
    then
      ADMIN_OK="yes"
    fi
  else
    warn "Neither curl nor python3 available to probe Admin UI"
  fi
fi

if [[ "$ADMIN_OK" == "yes" ]]; then
  pass "Admin UI reachable on localhost:9000"
else
  fail "Admin UI not reachable on localhost:9000"
fi

# --- Summary ---
print_header "Summary"
echo "PASS: $PASS_COUNT"
echo "WARN: $WARN_COUNT"
echo "FAIL: $FAIL_COUNT"

if [[ "$FAIL_COUNT" -gt 0 ]]; then
  echo
  echo "Failed checks:"
  for r in "${FAIL_REASONS[@]}"; do
    echo " - $r"
  done
  echo
  echo "Overall: FAIL"
  exit 1
else
  echo
  echo "Overall: PASS"
  exit 0
fi