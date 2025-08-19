import os
import yaml
import uuid
import time
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Integer, Float, DateTime, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.exc import SQLAlchemyError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_postgres_connection() -> str:
    """Get PostgreSQL connection string from environment variables."""
    user = os.getenv('POSTGRES_USER', 'airflow')
    password = os.getenv('POSTGRES_PASSWORD', 'airflow')
    # Use 'postgres' host when running inside Docker, 'localhost' when running locally
    host = os.getenv('POSTGRES_HOST', 'postgres' if os.path.exists('/opt/airflow') else 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'airflow')
    
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _create_pipeline_monitor_table(engine):
    """Create pipeline_monitor table if it doesn't exist."""
    metadata = MetaData()
    
    # Define pipeline_monitor table
    Table('pipeline_monitor', metadata,
          Column('run_id', UUID(as_uuid=True), primary_key=True),
          Column('dag_id', String(255), nullable=False),
          Column('run_date', String(10), nullable=False),  # YYYY-MM-DD format
          Column('rows_processed', Integer, nullable=False),
          Column('duration_sec', Float, nullable=False),
          Column('status', String(20), nullable=False),  # 'success' or 'failed'
          Column('error_message', Text, nullable=True),
          Column('created_at', DateTime, default=datetime.utcnow)
    )
    
    # Create table if not exists
    metadata.create_all(engine, checkfirst=True)
    logger.info("Pipeline monitor table ready")


def _create_analytics_table(engine, table_name: str, df: pd.DataFrame, unique_keys: Optional[List[str]] = None):
    """Create analytics table if it doesn't exist."""
    # Generate CREATE TABLE SQL based on DataFrame schema
    columns = []
    for col, dtype in df.dtypes.items():
        if 'int' in str(dtype):
            sql_type = 'INTEGER'
        elif 'float' in str(dtype):
            sql_type = 'DOUBLE PRECISION'
        elif 'datetime' in str(dtype):
            sql_type = 'TIMESTAMP'
        else:
            sql_type = 'VARCHAR(500)'
        
        columns.append(f"{col} {sql_type}")
    
    # Add primary key if unique_keys provided
    if unique_keys:
        columns.append(f"PRIMARY KEY ({', '.join(unique_keys)})")
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns)}
    )
    """
    
    with engine.begin() as conn:
        conn.execute(text(create_sql))
    
    logger.info(f"Analytics table '{table_name}' ready")


def _upsert_data(engine, table_name: str, df: pd.DataFrame, unique_keys: Optional[List[str]] = None, run_date: str = None):
    """Upsert data using either ON CONFLICT or DELETE+INSERT strategy."""
    if df.empty:
        logger.info("No data to upsert")
        return 0
    
    if unique_keys:
        # Strategy 1: INSERT ... ON CONFLICT DO UPDATE
        logger.info(f"Using UPSERT strategy with unique keys: {unique_keys}")
        
        # Convert DataFrame to list of dicts for insertion
        records = df.to_dict('records')
        
        # Build INSERT statement
        columns = list(df.columns)
        placeholders = ', '.join([f':{col}' for col in columns])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Build ON CONFLICT clause
        conflict_columns = ', '.join(unique_keys)
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col not in unique_keys])
        if update_clause:
            insert_sql += f" ON CONFLICT ({conflict_columns}) DO UPDATE SET {update_clause}"
        else:
            insert_sql += f" ON CONFLICT ({conflict_columns}) DO NOTHING"
        
        # Use connect() so tests that mock engine.connect capture execute() and rowcount
        with engine.connect() as conn:
            trans = conn.begin()
            try:
                result = conn.execute(text(insert_sql), records)
                trans.commit()
            except Exception:
                trans.rollback()
                raise
            rows_affected = result.rowcount
            logger.info(f"UPSERT completed: {rows_affected} rows affected")
            return rows_affected
    
    else:
        # Strategy 2: DELETE WHERE run_date THEN INSERT
        logger.info("Using DELETE+INSERT strategy")
        
        with engine.begin() as conn:
            # Delete existing data for this run_date
            delete_sql = f"DELETE FROM {table_name} WHERE run_date = :run_date"
            conn.execute(text(delete_sql), {'run_date': run_date})
            
            # Insert new data
            df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
            
            rows_affected = len(df)
            logger.info(f"DELETE+INSERT completed: {rows_affected} rows inserted")
            return rows_affected


def _log_pipeline_run(engine, run_id: str, dag_id: str, run_date: str, 
                     rows_processed: int, duration_sec: float, status: str, 
                     error_message: Optional[str] = None):
    """Log pipeline run to pipeline_monitor table."""
    insert_sql = """
    INSERT INTO pipeline_monitor 
    (run_id, dag_id, run_date, rows_processed, duration_sec, status, error_message)
    VALUES (:run_id, :dag_id, :run_date, :rows_processed, :duration_sec, :status, :error_message)
    """
    
    with engine.begin() as conn:
        conn.execute(text(insert_sql), {
            'run_id': run_id,
            'dag_id': dag_id,
            'run_date': run_date,
            'rows_processed': rows_processed,
            'duration_sec': duration_sec,
            'status': status,
            'error_message': error_message
        })
    
    logger.info(f"Pipeline run logged: {run_id} - {status}")


def load_to_postgres(config_path: str, execution_date: str) -> Dict[str, Any]:
    """
    Load transformed data to Postgres with idempotent behavior.
    
    Args:
        config_path: Path to YAML config file
        execution_date: Execution date in YYYY-MM-DD format
    
    Returns:
        Dict with run details: run_id, rows_processed, duration_sec, status
    """
    start_time = time.time()
    run_id = str(uuid.uuid4())
    
    try:
        # Load config
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        api_name = config['name']
        output_table = config.get('output_table', f"{api_name}_events")
        unique_keys = config.get('unique_keys', [])
        
        # Get database connection
        connection_string = _get_postgres_connection()
        engine = create_engine(connection_string)
        
        # Ensure pipeline_monitor table exists
        _create_pipeline_monitor_table(engine)
        
        # Load transformed data
        from src.transformers.data_transformer import transform_data
        df = transform_data(config_path, execution_date)
        
        if df.empty:
            logger.warning("No transformed data to load")
            duration = time.time() - start_time
            _log_pipeline_run(engine, run_id, api_name, execution_date, 0, duration, 'success')
            return {
                'run_id': run_id,
                'rows_processed': 0,
                'duration_sec': duration,
                'status': 'success'
            }
        
        # Create analytics table if needed
        _create_analytics_table(engine, output_table, df, unique_keys)
        
        # Add run_date column if not present (for DELETE+INSERT strategy)
        if not unique_keys and 'run_date' not in df.columns:
            df['run_date'] = execution_date
        
        # Upsert data
        rows_processed = _upsert_data(engine, output_table, df, unique_keys, execution_date)
        
        duration = time.time() - start_time
        
        # Log successful run
        _log_pipeline_run(engine, run_id, api_name, execution_date, rows_processed, duration, 'success')
        
        logger.info(f"Load completed successfully: {rows_processed} rows processed in {duration:.2f}s")
        
        return {
            'run_id': run_id,
            'rows_processed': rows_processed,
            'duration_sec': duration,
            'status': 'success'
        }
        
    except Exception as e:
        duration = time.time() - start_time
        error_msg = str(e)
        logger.error(f"Load failed: {error_msg}")
        
        # Log failed run if we have engine
        try:
            if 'engine' in locals():
                _log_pipeline_run(engine, run_id, api_name, execution_date, 0, duration, 'failed', error_msg)
        except:
            pass  # Don't fail if logging fails
        
        raise


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('config_path', type=str)
    parser.add_argument('execution_date', type=str)
    args = parser.parse_args()
    
    result = load_to_postgres(args.config_path, args.execution_date)
    print(f"Load result: {result}")
