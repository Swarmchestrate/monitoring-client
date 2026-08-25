import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from swchmonclient.deployer import _ensure_monitoring_manifest
from swchmonclient.exceptions import DeploymentError


def test_ensure_monitoring_manifest_downloads_missing_file(tmp_path):
    manifest_path = tmp_path / "manifests" / "emsconfig.yaml"
    logger = MagicMock(spec=logging.Logger)

    with patch.dict(
        "swchmonclient.deployer.MONITORING_MANIFEST_RELEASE_URLS",
        {str(manifest_path): "https://example.invalid/emsconfig.yaml"},
        clear=True,
    ), patch("swchmonclient.deployer._fetch_manifest_release_bytes", return_value=b"kind: ConfigMap\n"):
        _ensure_monitoring_manifest(str(manifest_path), logger)

    assert manifest_path.read_bytes() == b"kind: ConfigMap\n"
    logger.info.assert_any_call(
        "Fetching manifest %s from release asset %s.",
        str(manifest_path),
        "https://example.invalid/emsconfig.yaml",
    )
    logger.info.assert_any_call("Wrote manifest to %s.", str(manifest_path))


def test_ensure_monitoring_manifest_logs_match_for_existing_file(tmp_path):
    manifest_path = tmp_path / "manifests" / "emsconfig.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(b"same-content")
    logger = MagicMock(spec=logging.Logger)

    with patch.dict(
        "swchmonclient.deployer.MONITORING_MANIFEST_RELEASE_URLS",
        {str(manifest_path): "https://example.invalid/emsconfig.yaml"},
        clear=True,
    ), patch("swchmonclient.deployer._fetch_manifest_release_bytes", return_value=b"same-content"):
        _ensure_monitoring_manifest(str(manifest_path), logger)

    logger.info.assert_any_call("Found local manifest %s.", str(manifest_path))
    logger.info.assert_any_call("Local manifest %s matches release asset.", str(manifest_path))
    logger.warning.assert_not_called()


def test_ensure_monitoring_manifest_keeps_different_existing_file(tmp_path):
    manifest_path = tmp_path / "manifests" / "emsconfig.yaml"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_bytes(b"local-content")
    logger = MagicMock(spec=logging.Logger)

    with patch.dict(
        "swchmonclient.deployer.MONITORING_MANIFEST_RELEASE_URLS",
        {str(manifest_path): "https://example.invalid/emsconfig.yaml"},
        clear=True,
    ), patch("swchmonclient.deployer._fetch_manifest_release_bytes", return_value=b"release-content"):
        _ensure_monitoring_manifest(str(manifest_path), logger)

    assert manifest_path.read_bytes() == b"local-content"
    logger.warning.assert_called_once_with(
        "Local manifest %s differs from release asset %s. Keeping local file.",
        str(manifest_path),
        "https://example.invalid/emsconfig.yaml",
    )
    logger.info.assert_any_call("Found local manifest %s.", str(manifest_path))


def test_ensure_monitoring_manifest_raises_for_download_failure(tmp_path):
    manifest_path = tmp_path / "manifests" / "emsconfig.yaml"
    logger = MagicMock(spec=logging.Logger)

    with patch.dict(
        "swchmonclient.deployer.MONITORING_MANIFEST_RELEASE_URLS",
        {str(manifest_path): "https://example.invalid/emsconfig.yaml"},
        clear=True,
    ), patch(
        "swchmonclient.deployer._fetch_manifest_release_bytes",
        side_effect=DeploymentError("download failed"),
    ):
        with pytest.raises(DeploymentError, match="download failed"):
            _ensure_monitoring_manifest(str(manifest_path), logger)
