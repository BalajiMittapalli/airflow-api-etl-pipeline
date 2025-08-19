import os
import json
import yaml
import pandas as pd
from src.transformers.data_transformer import transform_data


def write_raw(tmpdir, api_name, execution_date, rows):
    raw_dir = os.path.join(tmpdir, 'data', 'raw', api_name, execution_date)
    os.makedirs(raw_dir, exist_ok=True)
    with open(os.path.join(raw_dir, 'response_001.json'), 'w') as f:
        json.dump(rows, f)


def make_config(tmpdir, api_name, mappings):
    cfg = {'name': api_name, 'mappings': mappings}
    cfg_path = os.path.join(tmpdir, 'cfg.yaml')
    with open(cfg_path, 'w') as f:
        yaml.safe_dump(cfg, f)
    return cfg_path


def test_nested_flatten_and_mapping(tmp_path):
    api = 'test_api'
    date = '2024-06-10'
    rows = [
        {'type': 'PushEvent', 'created_at': '2024-06-10T12:00:00Z', 'repo': {'id': 123}, 'actor': {'login': 'alice'}},
    ]
    mappings = [
        {'source': 'type', 'target': 'event_type', 'type': 'string'},
        {'source': 'created_at', 'target': 'event_time', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'source': 'repo.id', 'target': 'repo_id', 'type': 'int'},
        {'source': 'actor.login', 'target': 'user_login', 'type': 'string'},
    ]
    write_raw(tmp_path, api, date, rows)
    cfg_path = make_config(tmp_path, api, mappings)
    df = transform_data(cfg_path, date)
    assert list(df.columns) == ['event_type', 'event_time', 'repo_id', 'user_login', 'ingestion_timestamp', 'source']
    assert df.iloc[0]['event_type'] == 'PushEvent'
    assert pd.to_datetime(df.iloc[0]['event_time']) == pd.Timestamp('2024-06-10T12:00:00')
    assert df.iloc[0]['repo_id'] == 123
    assert df.iloc[0]['user_login'] == 'alice'
    assert df.iloc[0]['source'] == api


def test_type_conversion_error_skips_row(tmp_path):
    api = 'test_api'
    date = '2024-06-10'
    rows = [
        {'type': 'PushEvent', 'created_at': '2024-06-10T12:00:00Z', 'repo': {'id': 'not_int'}}
    ]
    mappings = [
        {'source': 'type', 'target': 'event_type', 'type': 'string'},
        {'source': 'created_at', 'target': 'event_time', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'source': 'repo.id', 'target': 'repo_id', 'type': 'int'},
    ]
    write_raw(tmp_path, api, date, rows)
    cfg_path = make_config(tmp_path, api, mappings)
    df = transform_data(cfg_path, date)
    assert len(df) == 0


def test_metadata_columns(tmp_path):
    api = 'test_api'
    date = '2024-06-10'
    rows = [
        {'type': 'X', 'created_at': '2024-06-10T00:00:00Z'}
    ]
    mappings = [
        {'source': 'type', 'target': 'event_type', 'type': 'string'},
        {'source': 'created_at', 'target': 'event_time', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
    ]
    write_raw(tmp_path, api, date, rows)
    cfg_path = make_config(tmp_path, api, mappings)
    df = transform_data(cfg_path, date)
    assert 'ingestion_timestamp' in df.columns
    assert 'source' in df.columns
