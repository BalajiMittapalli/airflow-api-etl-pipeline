import os
import json
import shutil
import tempfile
import yaml
import pandas as pd
import pytest
from src.validators import schema_validator

def make_raw_dir(tmpdir, api_name, execution_date, rows):
    raw_dir = os.path.join(tmpdir, 'data', 'raw', api_name, execution_date)
    os.makedirs(raw_dir, exist_ok=True)
    with open(os.path.join(raw_dir, 'response_001.json'), 'w') as f:
        json.dump(rows, f)
    return raw_dir

def make_config(tmpdir, api_name, schema=None):
    config = {
        'name': api_name,
    }
    if schema:
        config['schema'] = schema
    config_path = os.path.join(tmpdir, 'test_config.yaml')
    with open(config_path, 'w') as f:
        yaml.safe_dump(config, f)
    return config_path

def test_valid_data_passes(tmp_path):
    api_name = 'test_api'
    execution_date = '2024-06-10'
    rows = [
        {'id': 1, 'type': 'PushEvent', 'created_at': '2024-06-10T12:00:00Z'},
        {'id': 2, 'type': 'PullRequestEvent', 'created_at': '2024-06-10T13:00:00Z'}
    ]
    schema = {
        'required_columns': ['id', 'type', 'created_at'],
        'dtypes': {'id': 'int', 'type': 'string', 'created_at': 'datetime'},
        'validation': {'unique_keys': ['id'], 'non_null_fields': ['type']}
    }
    make_raw_dir(tmp_path, api_name, execution_date, rows)
    config_path = make_config(tmp_path, api_name, schema)
    report = schema_validator.validate_schema(config_path, execution_date)
    assert report['invalid_rows'] == 0
    assert report['valid_rows'] == 2

def test_missing_column_fails(tmp_path):
    api_name = 'test_api'
    execution_date = '2024-06-10'
    rows = [
        {'id': 1, 'created_at': '2024-06-10T12:00:00Z'}  # missing 'type'
    ]
    schema = {
        'required_columns': ['id', 'type', 'created_at'],
        'dtypes': {'id': 'int', 'type': 'string', 'created_at': 'datetime'},
        'validation': {'unique_keys': ['id'], 'non_null_fields': ['type']}
    }
    make_raw_dir(tmp_path, api_name, execution_date, rows)
    config_path = make_config(tmp_path, api_name, schema)
    report = schema_validator.validate_schema(config_path, execution_date)
    assert report['invalid_rows'] == 1
    assert any('type' in str(e) for e in report['errors'])

def test_type_mismatch_fails(tmp_path):
    api_name = 'test_api'
    execution_date = '2024-06-10'
    rows = [
        {'id': 'not_an_int', 'type': 'PushEvent', 'created_at': '2024-06-10T12:00:00Z'}
    ]
    schema = {
        'required_columns': ['id', 'type', 'created_at'],
        'dtypes': {'id': 'int', 'type': 'string', 'created_at': 'datetime'},
        'validation': {'unique_keys': ['id'], 'non_null_fields': ['type']}
    }
    make_raw_dir(tmp_path, api_name, execution_date, rows)
    config_path = make_config(tmp_path, api_name, schema)
    report = schema_validator.validate_schema(config_path, execution_date)
    assert report['invalid_rows'] == 1
    assert any('id' in str(e) for e in report['errors'])

def test_inference_works(tmp_path):
    api_name = 'test_api'
    execution_date = '2024-06-10'
    rows = [
        {'id': 1, 'type': 'PushEvent', 'created_at': '2024-06-10T12:00:00Z'},
        {'id': 2, 'type': 'PullRequestEvent', 'created_at': '2024-06-10T13:00:00Z'}
    ]
    make_raw_dir(tmp_path, api_name, execution_date, rows)
    config_path = make_config(tmp_path, api_name, schema=None)
    report = schema_validator.validate_schema(config_path, execution_date)
    assert 'inferred_schema' in report
    assert set(report['inferred_schema']['required_columns']) == {'id', 'type', 'created_at'}
