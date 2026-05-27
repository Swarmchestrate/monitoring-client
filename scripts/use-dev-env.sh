#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cp "${REPO_ROOT}/.env.dev" "${REPO_ROOT}/.env"
printf 'Activated local dev environment in %s/.env\n' "${REPO_ROOT}"
printf 'Next step: run scripts/port-forward-emsserver.sh while working outside the cluster.\n'

