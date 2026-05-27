#!/usr/bin/env bash
set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cp "${REPO_ROOT}/.env.cluster" "${REPO_ROOT}/.env"
printf 'Activated cluster environment in %s/.env\n' "${REPO_ROOT}"

