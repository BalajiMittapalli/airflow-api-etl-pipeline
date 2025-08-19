import os
import json
import yaml
import glob
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone


def _load_rows(raw_dir: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for file in sorted(glob.glob(os.path.join(raw_dir, '*.json'))):
        with open(file, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                rows.extend(data)
            elif isinstance(data, dict):
                rows.append(data)
    return rows


def _convert_series(series: pd.Series, target_type: str, fmt: Optional[str] = None,
                     scale: Optional[float] = None, offset: Optional[float] = None) -> pd.Series:
    if target_type == 'datetime':
        converted = pd.to_datetime(series, format=fmt, errors='coerce', utc=True)
        # Return naive UTC or keep timezone-aware? We'll make naive in UTC for analytics
        return converted.dt.tz_convert(None)
    if target_type == 'int':
        converted = pd.to_numeric(series, errors='coerce').astype('Int64')
    elif target_type == 'float':
        converted = pd.to_numeric(series, errors='coerce').astype('Float64')
    elif target_type == 'bool':
        # Handle various truthy/falsey strings
        truthy = {'true', '1', 'yes', 'y', 't'}
        falsy = {'false', '0', 'no', 'n', 'f'}
        def to_bool(x):
            if isinstance(x, bool):
                return x
            if pd.isna(x):
                return pd.NA
            s = str(x).strip().lower()
            if s in truthy:
                return True
            if s in falsy:
                return False
            return pd.NA
        converted = series.map(to_bool).astype('boolean')
    else:  # string
        converted = series.astype('string')
    # Apply unit conversions if provided
    if scale is not None:
        try:
            converted = converted * scale
        except Exception:
            converted = pd.Series([pd.NA] * len(series), dtype=converted.dtype)
    if offset is not None:
        try:
            converted = converted + offset
        except Exception:
            converted = pd.Series([pd.NA] * len(series), dtype=converted.dtype)
    return converted


def transform_data(config_path: str, execution_date: str) -> pd.DataFrame:
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    api_name = config['name']
    mappings: List[Dict[str, Any]] = config.get('mappings', [])

    # Resolve base directory robustly for both Docker and tests/local runs
    cfg_dir = os.path.dirname(os.path.abspath(config_path))
    candidates = []
    if os.path.exists('/opt/airflow'):
        candidates.append('/opt/airflow')
    candidates.append(cfg_dir)
    candidates.append(os.path.dirname(cfg_dir))
    base_dir = cfg_dir
    for cand in candidates:
        candidate_raw = os.path.join(cand, 'data', 'raw', api_name, execution_date)
        if os.path.isdir(candidate_raw):
            base_dir = cand
            break
    
    raw_dir = os.path.join(base_dir, 'data', 'raw', api_name, execution_date)
    invalid_dir = os.path.join(base_dir, 'data', 'invalid', api_name, execution_date)
    os.makedirs(invalid_dir, exist_ok=True)

    rows = _load_rows(raw_dir)
    if not rows:
        return pd.DataFrame(columns=[m['target'] for m in mappings] + ['ingestion_timestamp', 'source'])

    # Flatten nested structures with dot notation
    df_flat = pd.json_normalize(rows, sep='.')

    out_cols = [m['target'] for m in mappings]
    out_df = pd.DataFrame(index=df_flat.index)

    # Apply mappings
    invalid_mask = pd.Series(False, index=df_flat.index)
    for m in mappings:
        source = m['source']
        target = m['target']
        target_type = m.get('type', 'string')
        fmt = m.get('format')
        scale = m.get('scale')
        offset = m.get('offset')

        series = df_flat[source] if source in df_flat.columns else pd.Series([pd.NA] * len(df_flat), index=df_flat.index)
        converted = _convert_series(series, target_type, fmt=fmt, scale=scale, offset=offset)
        out_df[target] = converted
        # rows invalid if conversion resulted in NA for critical fields (all mapped fields considered critical)
        invalid_mask = invalid_mask | converted.isna()

    # Rows with any invalid mapped fields are dropped and logged
    invalid_rows_df = df_flat[invalid_mask]
    if len(invalid_rows_df) > 0:
        invalid_rows_df.to_json(os.path.join(invalid_dir, 'transform_invalid_rows.json'), orient='records', indent=2)
    clean_df = out_df[~invalid_mask].copy()

    # Add metadata
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    clean_df['ingestion_timestamp'] = now_utc
    clean_df['source'] = api_name

    # Ensure exact columns order
    clean_df = clean_df[out_cols + ['ingestion_timestamp', 'source']]

    return clean_df


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('config_path', type=str)
    parser.add_argument('execution_date', type=str)
    args = parser.parse_args()
    df = transform_data(args.config_path, args.execution_date)
    print(df.head().to_string(index=False))
