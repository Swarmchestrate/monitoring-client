import base64
import logging
from unittest.mock import MagicMock, patch

import pytest
import requests

from swchmonclient.deployer import _build_unique_sat_filename, _load_sat_file, _upload_sat_to_kb
from swchmonclient.exceptions import DeploymentError


def test_load_sat_file_returns_basename_and_content(tmp_path):
    sat_path = tmp_path / "nested" / "stressng.yaml"
    sat_path.parent.mkdir(parents=True, exist_ok=True)
    sat_path.write_text("topology: demo\n", encoding="utf-8")

    sat_filename, sat_content = _load_sat_file(str(sat_path))

    assert sat_filename == "stressng.yaml"
    assert sat_content == "topology: demo\n"


def test_build_unique_sat_filename_adds_timestamp_before_extension():
    unique_name = _build_unique_sat_filename("stressng.yaml")

    assert unique_name.startswith("stressng-")
    assert unique_name.endswith(".yaml")


def test_upload_sat_to_kb_posts_expected_payload():
    logger = MagicMock(spec=logging.Logger)
    response = MagicMock()

    with patch.dict(
        "os.environ",
        {
            "KB_BASE_URL": "http://optimusdb.example",
            "KB_CONTEXT": "swarmkb",
            "KB_TIMEOUT": "15",
        },
        clear=False,
    ), patch("swchmonclient.deployer.requests.post", return_value=response) as mock_post:
        _upload_sat_to_kb(
            sat_file="/tmp/stressng.yaml",
            sat_filename="stressng.yaml",
            sat_content="topology: demo\n",
            logger=logger,
        )

    mock_post.assert_called_once_with(
        "http://optimusdb.example/optimusdb1/swarmkb/upload",
        json={
            "file": base64.b64encode(b"topology: demo\n").decode("utf-8"),
            "filename": "stressng.yaml",
            "store_full_structure": True,
            "target_store": "dsswres",
        },
        timeout=15,
    )
    response.raise_for_status.assert_called_once_with()


def test_upload_sat_to_kb_wraps_request_errors():
    logger = MagicMock(spec=logging.Logger)

    with patch("swchmonclient.deployer.requests.post", side_effect=requests.RequestException("boom")):
        with pytest.raises(DeploymentError, match="Failed to upload SAT file /tmp/stressng.yaml"):
            _upload_sat_to_kb(
                sat_file="/tmp/stressng.yaml",
                sat_filename="stressng.yaml",
                sat_content="topology: demo\n",
                logger=logger,
            )
