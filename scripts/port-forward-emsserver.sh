#!/usr/bin/env bash
set -eu

NAMESPACE="${1:-default}"

kubectl port-forward -n "${NAMESPACE}" svc/emsserver-ems-server 61610:61610

