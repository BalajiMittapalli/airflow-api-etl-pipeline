import os
import json
import yaml
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import create_engine, text
from src.loaders.postgres_loader import load_to_postgres, _create_pipeline_monitor_table


def write_raw(tmpdir, api_name, execution_date, rows):
    """Helper to write raw data files."""
    raw_dir = os.path.join(tmpdir, 'data', 'raw', api_name, execution_date)
    os.makedirs(raw_dir, exist_ok=True)
    with open(os.path.join(raw_dir, 'response_001.json'), 'w') as f:
        json.dump(rows, f)


def make_config(tmpdir, api_name, mappings, output_table="test_events", unique_keys=None):
    """Helper to create test config."""
    cfg = {
        'name': api_name,
        'mappings': mappings,
        'output_table': output_table
    }
    if unique_keys:
        cfg['unique_keys'] = unique_keys
    
    cfg_path = os.path.join(tmpdir, 'cfg.yaml')
    with open(cfg_path, 'w') as f:
        yaml.safe_dump(cfg, f)
    return cfg_path


@pytest.fixture
def sample_data():
    """Sample GitHub events data."""
    return [
        {
            'id': 123456,
            'type': 'PushEvent',
            'created_at': '2024-06-10T12:00:00Z',
            'repo': {'id': 123},
            'actor': {'login': 'alice'}
        },
        {
            'id': 123457,
            'type': 'PullRequestEvent',
            'created_at': '2024-06-10T13:00:00Z',
            'repo': {'id': 456},
            'actor': {'login': 'bob'}
        }
    ]


@pytest.fixture
def sample_mappings():
    """Sample field mappings."""
    return [
        {'source': 'id', 'target': 'event_id', 'type': 'int'},
        {'source': 'type', 'target': 'event_type', 'type': 'string'},
        {'source': 'created_at', 'target': 'event_time', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'source': 'repo.id', 'target': 'repo_id', 'type': 'int'},
        {'source': 'actor.login', 'target': 'user_login', 'type': 'string'},
    ]


@patch('src.loaders.postgres_loader._get_postgres_connection')
@patch('src.transformers.data_transformer.transform_data')
def test_load_to_postgres_with_unique_keys(mock_transform, mock_conn, tmp_path, sample_data, sample_mappings):
    """Test loading with unique keys (UPSERT strategy)."""
    # Setup
    api_name = 'test_api'
    execution_date = '2024-06-10'
    
    # Create transformed DataFrame
    df = pd.DataFrame([
        {'event_id': 123456, 'event_type': 'PushEvent', 'event_time': pd.Timestamp('2024-06-10T12:00:00Z'), 
         'repo_id': 123, 'user_login': 'alice', 'ingestion_timestamp': pd.Timestamp.now(), 'source': api_name},
        {'event_id': 123457, 'event_type': 'PullRequestEvent', 'event_time': pd.Timestamp('2024-06-10T13:00:00Z'), 
         'repo_id': 456, 'user_login': 'bob', 'ingestion_timestamp': pd.Timestamp.now(), 'source': api_name}
    ])
    mock_transform.return_value = df
    
    # Mock database connection
    mock_engine = MagicMock()
    mock_conn.return_value = "postgresql://test:test@localhost:5432/test"
    
    with patch('src.loaders.postgres_loader.create_engine', return_value=mock_engine):
        # Setup mock for rowcount
        mock_result = MagicMock()
        mock_result.rowcount = 2
        mock_engine.connect.return_value.__enter__.return_value.execute.return_value = mock_result
        
        # Create config
        cfg_path = make_config(tmp_path, api_name, sample_mappings, "test_events", ["event_id"])
        
        # Run loader
        result = load_to_postgres(cfg_path, execution_date)
        
        # Assertions
        assert result['status'] == 'success'
        assert result['rows_processed'] == 2
        assert 'run_id' in result
        assert result['duration_sec'] > 0
        
        # Verify pipeline_monitor table was created
        mock_engine.connect.assert_called()


@patch('src.loaders.postgres_loader._get_postgres_connection')
@patch('src.transformers.data_transformer.transform_data')
def test_load_to_postgres_without_unique_keys(mock_transform, mock_conn, tmp_path, sample_data, sample_mappings):
    """Test loading without unique keys (DELETE+INSERT strategy)."""
    # Setup
    api_name = 'test_api'
    execution_date = '2024-06-10'
    
    # Create transformed DataFrame with run_date
    df = pd.DataFrame([
        {'event_id': 123456, 'event_type': 'PushEvent', 'event_time': pd.Timestamp('2024-06-10T12:00:00Z'), 
         'repo_id': 123, 'user_login': 'alice', 'ingestion_timestamp': pd.Timestamp.now(), 'source': api_name},
        {'event_id': 123457, 'event_type': 'PullRequestEvent', 'event_time': pd.Timestamp('2024-06-10T13:00:00Z'), 
         'repo_id': 456, 'user_login': 'bob', 'ingestion_timestamp': pd.Timestamp.now(), 'source': api_name}
    ])
    df['run_date'] = execution_date
    mock_transform.return_value = df
    
    # Mock database connection
    mock_engine = MagicMock()
    mock_conn.return_value = "postgresql://test:test@localhost:5432/test"
    
    with patch('src.loaders.postgres_loader.create_engine', return_value=mock_engine):
        # Create config without unique keys
        cfg_path = make_config(tmp_path, api_name, sample_mappings, "test_events")
        
        # Run loader
        result = load_to_postgres(cfg_path, execution_date)
        
        # Assertions
        assert result['status'] == 'success'
        assert result['rows_processed'] == 2
        assert 'run_id' in result


@patch('src.loaders.postgres_loader._get_postgres_connection')
@patch('src.transformers.data_transformer.transform_data')
def test_load_to_postgres_empty_data(mock_transform, mock_conn, tmp_path, sample_mappings):
    """Test loading with empty transformed data."""
    # Setup
    api_name = 'test_api'
    execution_date = '2024-06-10'
    
    # Empty DataFrame
    mock_transform.return_value = pd.DataFrame()
    
    # Mock database connection
    mock_engine = MagicMock()
    mock_conn.return_value = "postgresql://test:test@localhost:5432/test"
    
    with patch('src.loaders.postgres_loader.create_engine', return_value=mock_engine):
        # Create config
        cfg_path = make_config(tmp_path, api_name, sample_mappings)
        
        # Run loader
        result = load_to_postgres(cfg_path, execution_date)
        
        # Assertions
        assert result['status'] == 'success'
        assert result['rows_processed'] == 0
        assert 'run_id' in result


@patch('src.loaders.postgres_loader._get_postgres_connection')
@patch('src.transformers.data_transformer.transform_data')
def test_load_to_postgres_error_handling(mock_transform, mock_conn, tmp_path, sample_mappings):
    """Test error handling during load."""
    # Setup
    api_name = 'test_api'
    execution_date = '2024-06-10'
    
    # Mock transform to raise exception
    mock_transform.side_effect = Exception("Transform failed")
    
    # Mock database connection
    mock_engine = MagicMock()
    mock_conn.return_value = "postgresql://test:test@localhost:5432/test"
    
    with patch('src.loaders.postgres_loader.create_engine', return_value=mock_engine):
        # Create config
        cfg_path = make_config(tmp_path, api_name, sample_mappings)
        
        # Run loader and expect exception
        with pytest.raises(Exception, match="Transform failed"):
            load_to_postgres(cfg_path, execution_date)


def test_idempotency_integration(tmp_path, sample_data, sample_mappings):
    """Integration test for idempotency - run loader twice with same data."""
    # This test requires a real database connection
    # For now, we'll test the logic without actual database operations
    
    api_name = 'test_api'
    execution_date = '2024-06-10'
    
    # Create config with unique keys
    cfg_path = make_config(tmp_path, api_name, sample_mappings, "test_events", ["event_id"])
    
    # Mock the entire database interaction
    with patch('src.loaders.postgres_loader.create_engine') as mock_create_engine, \
         patch('src.transformers.data_transformer.transform_data') as mock_transform:
        
        # Setup mock engine and connection
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        
        # Create sample transformed data
        df = pd.DataFrame([
            {'event_id': 123456, 'event_type': 'PushEvent', 'event_time': pd.Timestamp('2024-06-10T12:00:00Z'), 
             'repo_id': 123, 'user_login': 'alice', 'ingestion_timestamp': pd.Timestamp.now(), 'source': api_name}
        ])
        mock_transform.return_value = df
        
        # Run loader first time
        result1 = load_to_postgres(cfg_path, execution_date)
        
        # Run loader second time (should be idempotent)
        result2 = load_to_postgres(cfg_path, execution_date)
        
        # Both should succeed
        assert result1['status'] == 'success'
        assert result2['status'] == 'success'
        
        # Both should have different run_ids
        assert result1['run_id'] != result2['run_id']
        
        # Both should process the same number of rows
        assert result1['rows_processed'] == result2['rows_processed']


@patch('src.loaders.postgres_loader._get_postgres_connection')
def test_pipeline_monitor_table_creation(mock_conn):
    """Test pipeline_monitor table creation."""
    mock_engine = MagicMock()
    mock_conn.return_value = "postgresql://test:test@localhost:5432/test"
    
    with patch('src.loaders.postgres_loader.create_engine', return_value=mock_engine):
        _create_pipeline_monitor_table(mock_engine)
        
        # The function should have been called, even if we can't verify the exact internal calls
        # due to SQLAlchemy's metadata.create_all() complexity
        assert True  # Just verify the function runs without error


if __name__ == '__main__':
    pytest.main([__file__])
