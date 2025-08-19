import os
import sys
import json
import time
import yaml
import requests
from datetime import datetime
from typing import List, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from airflow.hooks.base import BaseHook
except ImportError:
    BaseHook = None  # For local testing without Airflow

def get_airflow_connection(conn_id: str):
    if BaseHook is None:
        raise RuntimeError("Airflow not available. Cannot fetch connection.")
    conn = BaseHook.get_connection(conn_id)
    return conn

def extract_api(config_path: str, execution_date: Optional[str] = None) -> List[str]:
    # Load config
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    api_name = config['name']
    base_url = config['base_url']
    endpoint = config['endpoint']
    params = config.get('params', {})
    auth_cfg = config.get('auth', {'type': 'none'})
    pagination = config.get('pagination', None)
    rate_limit = config.get('rate_limit', {})
    requests_per_minute = rate_limit.get('requests_per_minute', 60)
    sleep_time = 60.0 / requests_per_minute if requests_per_minute else 0

    # Set execution date
    if not execution_date:
        execution_date = datetime.now().strftime('%Y-%m-%d')

    # Prepare output dir
    out_dir = os.path.join('data', 'raw', api_name, execution_date)
    os.makedirs(out_dir, exist_ok=True)

    # Setup requests session with retries
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))

    # Handle auth
    headers = {}
    if auth_cfg['type'] == 'api_key':
        key_name = auth_cfg['key_name']
        conn_id = auth_cfg['conn_id']
        if BaseHook is not None:
            conn = get_airflow_connection(conn_id)
            api_key = conn.password or conn.extra_dejson.get('api_key')
        else:
            api_key = os.environ.get(key_name)
        headers[key_name] = api_key
    elif auth_cfg['type'] == 'bearer':
        conn_id = auth_cfg['conn_id']
        if BaseHook is not None:
            conn = get_airflow_connection(conn_id)
            token = conn.password or conn.extra_dejson.get('token')
        else:
            token = os.environ.get('BEARER_TOKEN')
        headers['Authorization'] = f'Bearer {token}'
    # else: no auth

    saved_files = []
    page_num = pagination.get('start_page', 1) if pagination else 1
    cursor = None
    has_more = True
    response_idx = 1
    url = base_url.rstrip('/') + endpoint
    while has_more:
        req_params = params.copy()
        # Pagination logic
        if pagination:
            if pagination['type'] == 'page':
                req_params[pagination['page_param']] = page_num
            elif pagination['type'] == 'cursor' and cursor:
                req_params[pagination['cursor_param']] = cursor
        try:
            resp = session.get(url, params=req_params, headers=headers, timeout=30)
        except Exception as e:
            print(f"Request failed: {e}")
            break
        if resp.status_code == 401:
            print("Authentication failed (401). Exiting.")
            break
        if resp.status_code >= 400:
            print(f"HTTP error {resp.status_code}: {resp.text}")
            break
        data = resp.json()
        # For page-based pagination, only save non-empty responses
        if pagination and pagination['type'] == 'page':
            if not data or (isinstance(data, list) and len(data) == 0):
                has_more = False
                break  # Do not save empty response
            else:
                out_path = os.path.join(out_dir, f"response_{response_idx:03d}.json")
                with open(out_path, 'w') as f:
                    json.dump(data, f, indent=2)
                saved_files.append(out_path)
                response_idx += 1
                page_num += 1
        else:
            # For other pagination types, save all responses
            out_path = os.path.join(out_dir, f"response_{response_idx:03d}.json")
            with open(out_path, 'w') as f:
                json.dump(data, f, indent=2)
            saved_files.append(out_path)
            response_idx += 1
            if pagination:
                if pagination['type'] == 'cursor':
                    cursor = data.get(pagination['cursor_key'])
                    has_more = bool(cursor)
                elif pagination['type'] == 'next_link':
                    next_link = data
                    for key in pagination['next_link_key'].split('.'):
                        next_link = next_link.get(key, None)
                        if next_link is None:
                            break
                    if next_link:
                        url = next_link
                    else:
                        has_more = False
                else:
                    has_more = False
            else:
                has_more = False
        if has_more and sleep_time > 0:
            time.sleep(sleep_time)
    return saved_files

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("config_path", type=str, help="Path to YAML config file")
    parser.add_argument("--execution_date", type=str, default=None, help="Execution date (YYYY-MM-DD)")
    args = parser.parse_args()
    files = extract_api(args.config_path, args.execution_date)
    print("Saved files:", files)
