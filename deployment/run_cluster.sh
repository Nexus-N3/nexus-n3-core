#!/usr/bin/env bash
set -e

# -------- configuration --------
SITE="my_house"
LOCAL_GATEWAY="zeromq_gateway"
WORKERS=("worker_A") #"worker_B")
# --------------------------------

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PIDS=()

cleanup() {
  echo
  echo "[cluster] shutting down..."
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait
  echo "[cluster] stopped"
}

trap cleanup SIGINT SIGTERM EXIT

# Ensure logs/outputs are created under the project root.
cd "$ROOT_DIR"

# Start master
echo "[cluster] starting master"
python "$ROOT_DIR/nexus_n3_server.py" --gateway "$LOCAL_GATEWAY" --site "$SITE" --use-async --role master --admin &
PIDS+=($!)

# Give master time to bind / advertise
sleep 2

# Start workers
for worker in "${WORKERS[@]}"; do
  echo "[cluster] starting worker $worker"
  python "$ROOT_DIR/nexus_n3_server.py" --gateway "$LOCAL_GATEWAY" --site "$SITE" --use-async --role worker --node-id "$worker" &
  PIDS+=($!)
done

# Block until all processes exit
wait
