import os
import shutil
import tempfile
import pytest
import yaml
from unittest import mock
from src.extractors import api_adapter

def make_config(tmpdir, pagination_type='page', empty_on_page=3):
    config = {
        'name': 'test_api',
        'base_url': 'http://example.com',
        'endpoint': '/data',
        'params': {},
        'auth': {'type': 'none'},
        'pagination': {
            'type': pagination_type,
            'page_param': 'page',
            'start_page': 1
        },
        'rate_limit': {'requests_per_minute': 120}
    }
    config_path = os.path.join(tmpdir, 'test_config.yaml')
    with open(config_path, 'w') as f:
        yaml.safe_dump(config, f)
    return config_path

def mock_response(json_data, status=200):
    m = mock.Mock()
    m.status_code = status
    m.json.return_value = json_data
    return m

@mock.patch('requests.Session.get')
def test_pagination_stops_on_empty(mock_get, tmp_path):
    # Page 1, 2 have data, 3 is empty
    mock_get.side_effect = [
        mock_response([{'id': 1}]),
        mock_response([{'id': 2}]),
        mock_response([])
    ]
    config_path = make_config(tmp_path)
    files = api_adapter.extract_api(config_path)
    assert len(files) == 2
    for f in files:
        assert os.path.exists(f)

@mock.patch('requests.Session.get')
def test_file_paths_and_rate_limit(mock_get, tmp_path):
    # Only one page
    mock_get.side_effect = [mock_response([{'id': 1}]), mock_response([])]
    config_path = make_config(tmp_path)
    with mock.patch('time.sleep') as mock_sleep:
        files = api_adapter.extract_api(config_path)
        assert len(files) == 1
        assert files[0].endswith('response_001.json')
        mock_sleep.assert_called()
