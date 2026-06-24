#!/usr/bin/env python3
"""Upload a SAT (Swarm Application Template) YAML file into the Swarmchestrate KB."""

import base64
import os
import sys

import requests


def upload_sat(path: str) -> None:
    filename = os.path.basename(path)

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    base = os.environ.get("KB_BASE_URL", "http://optimusdb.swarmchestrate.sztaki.hu").rstrip("/")
    context = os.environ.get("KB_CONTEXT", "swarmkb").strip("/")
    timeout = int(os.environ.get("KB_TIMEOUT", 10))
    upload_url = f"{base}/optimusdb1/{context}/upload"

    payload = {
        "file": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "filename": filename,
        "store_full_structure": True,
        "target_store": "dsswres",
    }

    response = requests.post(upload_url, json=payload, timeout=timeout)
    response.raise_for_status()
    print(f"'{filename}' uploaded successfully to the KB.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python upload_sat.py <path-to-sat.yaml>")
    upload_sat(sys.argv[1])