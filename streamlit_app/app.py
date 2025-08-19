import os
import glob
import json
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import yaml


# ------------------
# Config & Setup
# ------------------
load_dotenv()

st.set_page_config(page_title="ETL Monitoring", layout="wide")

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")

PG_USER = os.getenv("POSTGRES_USER", "airflow")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "airflow")

REFRESH_SEC = int(os.getenv("DASHBOARD_REFRESH_SEC", "30"))


def get_engine():
	uri = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
	return create_engine(uri)


def list_api_configs():
	files = sorted(glob.glob(os.path.join("/app", "..", "configs", "*.yaml")))
	# fallback for running on host
	if not files:
		files = sorted(glob.glob(os.path.join("configs", "*.yaml")))
	return [os.path.splitext(os.path.basename(f))[0] for f in files]


def fetch_runs(limit=50):
	try:
		with get_engine().connect() as conn:
			sql = text(
				"""
				SELECT dag_id, run_date, rows_processed, duration_sec, status, created_at
				FROM pipeline_monitor
				ORDER BY created_at DESC
				LIMIT :lim
				"""
			)
			df = pd.read_sql(sql, conn, params={"lim": limit})
		return df
	except Exception as e:
		st.error(f"Failed to fetch runs: {e}")
		return pd.DataFrame()


def trigger_dag(api_config: str):
	url = f"{AIRFLOW_BASE_URL}/api/v1/dags/api_etl_dag/dagRuns"
	payload = {"conf": {"api_config": api_config}}
	try:
		resp = requests.post(url, json=payload, auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD), timeout=15)
		if resp.status_code >= 400:
			try:
				detail = resp.json()
			except Exception:
				detail = resp.text
			st.error(f"Airflow API error {resp.status_code}: {detail}")
		else:
			st.success("DAG triggered successfully")
	except Exception as e:
		st.error(f"Failed to call Airflow API: {e}")


# ------------------
# Sidebar / Header
# ------------------
st.title("ETL Monitoring & Control")
st.caption("Config-driven, API-agnostic ETL with Airflow 2.9")

# Auto-refresh
auto_refresh = st.sidebar.checkbox("Auto-refresh", value=True)
st.sidebar.caption(f"Auto-refresh: every {REFRESH_SEC}s")
if auto_refresh:
		# Lightweight JS-based refresh (avoids deprecated experimental APIs)
		components.html(
				f"""
				<script>
					setTimeout(function() {{ window.location.reload(); }}, {REFRESH_SEC * 1000});
				</script>
				""",
				height=0,
		)


# ------------------
# API Config Selector
# ------------------
configs = list_api_configs()
col_select, col_btn = st.columns([3, 1])
with col_select:
	selected_cfg = st.selectbox("API Config", options=configs, index=0 if configs else None)
with col_btn:
	if st.button("Run Now", type="primary", disabled=not selected_cfg):
		trigger_dag(selected_cfg)

st.divider()


# ------------------
# Run History/Table
# ------------------
runs = fetch_runs(limit=100)
if not runs.empty:
	# Status badge coloring
	def badge(s):
		color = "green" if s == "success" else "red"
		return f"<span style='color:{color};font-weight:600'>{s}</span>"

	st.subheader("Recent Pipeline Runs")
	show = runs.head(10).copy()
	show["status"] = show["status"].apply(badge)
	st.write(show.to_html(escape=False, index=False), unsafe_allow_html=True)
else:
	st.info("No runs yet.")


# ------------------
# KPIs
# ------------------
st.subheader("KPIs")
col1, col2, col3 = st.columns(3)

if not runs.empty:
	# Rows processed time series
	with col1:
		ts = runs.sort_values("created_at").copy()
		fig = px.line(ts, x="created_at", y="rows_processed", title="Rows Processed Over Time")
		st.plotly_chart(fig, use_container_width=True)

	# Success rate last 7 days
	with col2:
		cutoff = datetime.utcnow() - timedelta(days=7)
		last7 = runs[pd.to_datetime(runs["created_at"]) >= cutoff]
		if not last7.empty:
			rate = (last7["status"] == "success").mean() * 100
			st.metric("Success Rate (7d)", f"{rate:.1f}%")
		else:
			st.metric("Success Rate (7d)", "N/A")

	# Placeholder anomaly count
	with col3:
		st.metric("Anomaly Count", "0")
else:
	col1.metric("Rows Processed", "0")
	col2.metric("Success Rate (7d)", "N/A")
	col3.metric("Anomaly Count", "0")

st.divider()


# ------------------
# Anomalies placeholder (editable)
# ------------------
st.subheader("Anomalies (placeholder)")
anom = pd.DataFrame({
	"timestamp": [],
	"description": [],
	"severity": [],
})
st.data_editor(anom, num_rows="dynamic", use_container_width=True)


# ------------------
# Cache hygiene (new caching APIs)
# ------------------
try:
	st.cache_resource.clear()
	st.cache_data.clear()
except Exception:
	# Older Streamlit versions: no-op
	pass
