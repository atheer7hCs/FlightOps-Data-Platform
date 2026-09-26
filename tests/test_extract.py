import sys
import json
from pathlib import Path
import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent / "src"))

from extract import build_partitioned_path, save_raw_json, fetch_flight_data

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


def test_build_partitioned_path_creates_correct_structure(tmp_path):
    run_time = datetime(2026, 9, 16, tzinfo=timezone.utc)

    result_path = build_partitioned_path(tmp_path, run_time)

    expected = tmp_path / "year=2026" / "month=09" / "day=16"
    assert result_path == expected
    assert result_path.exists()





def test_save_raw_json_writes_valid_json(tmp_path):
    fake_payload = {"time": 123456, "states": [["abc123", "TEST1", "Saudi Arabia"]]}
    run_time = datetime(2026, 9, 16, 10, 30, 0, tzinfo=timezone.utc)

    file_path = save_raw_json(fake_payload, tmp_path, run_time)

    assert file_path.exists()
    with open(file_path) as f:
        saved_data = json.load(f)
    assert saved_data == fake_payload
    assert "flights_2026-09-16T10-30-00.json" in str(file_path)




@patch("extract.requests.get")
def test_fetch_flight_data_success(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {"time": 123, "states": [["abc", "TEST"]]}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = fetch_flight_data()

    assert result["time"] == 123
    assert len(result["states"]) == 1
    mock_get.assert_called_once()


@patch("extract.requests.get")
def test_fetch_flight_data_raises_on_http_error(mock_get):
    import requests

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 error")
    mock_get.return_value = mock_response

    with pytest.raises(requests.exceptions.HTTPError):
        fetch_flight_data()

