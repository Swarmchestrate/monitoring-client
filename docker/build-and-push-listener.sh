#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="emodimark/listener"
IMAGE_TAG="${1:-latest}"
FULL_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "Building ${FULL_IMAGE} from ${REPO_ROOT} ..."
docker build \
  -f "${SCRIPT_DIR}/Dockerfile-listener" \
  -t "${FULL_IMAGE}" \
  "${REPO_ROOT}"

echo "Pushing ${FULL_IMAGE} to Docker Hub ..."
docker push "${FULL_IMAGE}"

echo "Done: ${FULL_IMAGE}"
