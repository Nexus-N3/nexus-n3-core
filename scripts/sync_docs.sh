#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC=""
if [[ -d "${ROOT_DIR}/docs/_build/html" ]]; then
  SRC="${ROOT_DIR}/docs/_build/html"
elif [[ -d "${ROOT_DIR}/docs/build/html" ]]; then
  SRC="${ROOT_DIR}/docs/build/html"
elif [[ -d "${ROOT_DIR}/docs/build" ]]; then
  SRC="${ROOT_DIR}/docs/build"
fi
DEST="${ROOT_DIR}/nexus_n3.admin/docs/html"

if [[ -z "${SRC}" || ! -d "${SRC}" ]]; then
  echo "Docs not built. Run: cd docs && make html" >&2
  exit 1
fi

mkdir -p "${DEST}"
rm -rf "${DEST:?}"/*
cp -a "${SRC}/." "${DEST}/"
