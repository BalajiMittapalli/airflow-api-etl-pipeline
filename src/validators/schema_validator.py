import os
import sys
import yaml
import json
import glob
import pandas as pd
try:
    import pandera.pandas as pa  # new namespaced import
    try:
        from pandera.errors import SchemaErrors
    except Exception:  # fallback if errors module path changes
        SchemaErrors = pa.errors.SchemaErrors  # type: ignore[attr-defined]
except Exception:  # fallback for older pandera versions
    import pandera as pa
    try:
        from pandera.errors import SchemaErrors
    except Exception:  # very old versions
        SchemaErrors = pa.errors.SchemaErrors  # type: ignore[attr-defined]
from typing import Dict, Any
from datetime import datetime

def infer_dtype(series):
    if pd.api.types.is_integer_dtype(series):
        return "int"
    elif pd.api.types.is_float_dtype(series):
        return "float"
    elif pd.api.types.is_bool_dtype(series):
        return "bool"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    else:
        return "string"

def infer_schema(df: pd.DataFrame) -> Dict[str, Any]:
    dtypes = {col: infer_dtype(df[col]) for col in df.columns}
    return {
        "required_columns": list(df.columns),
        "dtypes": dtypes,
        "validation": {
            "unique_keys": [],
            "non_null_fields": []
        }
    }

def dtype_to_pandera(dtype):
    return {
        "int": pa.Int,
        "float": pa.Float,
        "string": pa.String,
        "datetime": pa.DateTime,
        "bool": pa.Bool
    }[dtype]

def validate_schema(config_path: str, execution_date: str) -> dict:
    # Load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    api_name = config['name']
    schema_cfg = config.get('schema')

    # Resolve base directory robustly for both Docker and tests/local runs
    cfg_dir = os.path.dirname(os.path.abspath(config_path))
    candidates = []
    if os.path.exists('/opt/airflow'):
        candidates.append('/opt/airflow')
    candidates.append(cfg_dir)
    candidates.append(os.path.dirname(cfg_dir))
    base_dir = cfg_dir
    # The expected raw dir is data/raw/<api>/<date>
    for cand in candidates:
        candidate_raw = os.path.join(cand, 'data', 'raw', api_name, execution_date)
        if os.path.isdir(candidate_raw):
            base_dir = cand
            break

    raw_dir = os.path.join(base_dir, 'data', 'raw', api_name, execution_date)
    invalid_dir = os.path.join(base_dir, 'data', 'invalid', api_name, execution_date)
    os.makedirs(invalid_dir, exist_ok=True)

    files = sorted(glob.glob(os.path.join(raw_dir, '*.json')))
    all_rows = []
    for file in files:
        with open(file, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                all_rows.extend(data)
            elif isinstance(data, dict):
                all_rows.append(data)
    if not all_rows:
        return {"valid_rows": 0, "invalid_rows": 0, "errors": ["No data found."]}

    df = pd.DataFrame(all_rows)
    errors = []
    valid_rows = 0
    invalid_rows = 0

    if schema_cfg:
        # Build Pandera schema
        columns = {}
        non_null_fields = set(schema_cfg.get('validation', {}).get('non_null_fields', []))
        for col in schema_cfg['required_columns']:
            dtype = schema_cfg['dtypes'][col]
            columns[col] = pa.Column(dtype_to_pandera(dtype), nullable=(col not in non_null_fields))
        schema = pa.DataFrameSchema(columns, coerce=True)
        # Validate
        try:
            validated = schema.validate(df, lazy=True)
            valid_rows = len(validated)
        except SchemaErrors as e:
            failure_cases = e.failure_cases
            # Extract only valid integer indices from failure cases
            raw_indices = failure_cases.get('index') if 'index' in failure_cases else []
            if hasattr(raw_indices, 'tolist'):
                raw_indices = raw_indices.tolist()
            valid_indices = [i for i in raw_indices if isinstance(i, int)]
            if len(valid_indices) > 0:
                invalid_idx_set = set(valid_indices)
                valid_rows = len(df) - len(invalid_idx_set)
                invalid_rows = len(invalid_idx_set)
                # Log invalid rows by index
                invalid_df = df.loc[sorted(invalid_idx_set)]
                invalid_df.to_json(os.path.join(invalid_dir, 'invalid_rows.json'), orient='records', indent=2)
            else:
                # When no row index is provided (e.g., missing column), treat all rows as invalid
                valid_rows = 0
                invalid_rows = len(df)
                df.to_json(os.path.join(invalid_dir, 'invalid_rows.json'), orient='records', indent=2)
            errors = failure_cases.to_dict('records')
            # Fail if >5% invalid
            if len(df) > 0 and invalid_rows / len(df) > 0.05:
                return {"valid_rows": valid_rows, "invalid_rows": invalid_rows, "errors": errors}
        # Unique keys
        unique_keys = schema_cfg.get('validation', {}).get('unique_keys', [])
        if unique_keys:
            if df.duplicated(subset=unique_keys).any():
                errors.append(f"Duplicate values found in unique_keys: {unique_keys}")
        # Non-null fields
        for field in schema_cfg.get('validation', {}).get('non_null_fields', []):
            if df[field].isnull().any():
                errors.append(f"Null values found in non_null_field: {field}")
        return {"valid_rows": valid_rows, "invalid_rows": invalid_rows, "errors": errors}
    else:
        # Infer schema
        sample_df = df.head(100)
        inferred = infer_schema(sample_df)
        inferred_dir = os.path.join(base_dir, 'configs', 'inferred_schemas')
        os.makedirs(inferred_dir, exist_ok=True)
        with open(os.path.join(inferred_dir, f"{api_name}.yaml"), 'w') as f:
            yaml.safe_dump(inferred, f)
        return {"valid_rows": len(df), "invalid_rows": 0, "errors": [], "inferred_schema": inferred}

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", type=str, help="Path to YAML config file")
    parser.add_argument("execution_date", type=str, help="Execution date (YYYY-MM-DD)")
    args = parser.parse_args()
    report = validate_schema(args.config_path, args.execution_date)
    print(json.dumps(report, indent=2))
