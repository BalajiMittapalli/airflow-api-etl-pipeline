# Airflow ETL Pipeline

A configurable ETL pipeline built with Apache Airflow for extracting data from REST APIs, validating schemas, transforming data, and loading into PostgreSQL.

## Features

- **Config-driven**: Define APIs, schemas, and transformations via YAML
- **Schema validation**: Pandera-based validation with configurable rules
- **Data transformation**: Nested JSON flattening with type conversion
- **Database loading**: UPSERT strategies for PostgreSQL with monitoring
- **Error handling**: Comprehensive error tracking and invalid data isolation
- **Monitoring**: Pipeline execution tracking and Slack notifications
- **Streamlit dashboard**: Web UI for monitoring and triggering pipelines

## Architecture

```
Extract → Validate → Transform → Load
   ↓         ↓          ↓        ↓
 Raw      Invalid   Processed  PostgreSQL
 JSON     Records   DataFrames  Tables
```

## Quick Start

1. **Clone and setup**:
   ```bash
   git clone https://github.com/BalajiMittapalli/airflow-api-etl-pipeline.git
   cd airflow-api-etl-pipeline
   ```

2. **Environment setup**:
   ```bash
   cp .env.example .env
   # Edit .env with your configurations
   ```

3. **Start services**:
   ```bash
   docker compose up -d
   ```

4. **Access interfaces**:
   - Airflow: http://localhost:8080 (admin/admin)
   - Streamlit: http://localhost:8501
   - MinIO: http://localhost:9001

## Configuration

### API Configuration (`configs/*.yaml`)

```yaml
name: "github_events"
description: "GitHub public events API"
schedule: "@hourly"
base_url: "https://api.github.com"
endpoint: "/events"
params:
  per_page: 100
auth:
  type: "none"
pagination:
  type: "page"
  page_param: "page"
  start_page: 1
rate_limit:
  requests_per_minute: 30
schema:
  required_columns: ["id", "type", "created_at"]
  dtypes:
    id: "string"
    type: "string"
    created_at: "datetime"
  validation:
    unique_keys: ["id"]
    non_null_fields: ["type"]
mappings:
  - source: "id"
    target: "event_id"
    type: "string"
  - source: "type"
    target: "event_type"
    type: "string"
  - source: "created_at"
    target: "event_time"
    type: "datetime"
    format: "%Y-%m-%dT%H:%M:%SZ"
  - source: "repo.id"
    target: "repo_id"
    type: "int"
  - source: "actor.login"
    target: "user_login"
    type: "string"
output_table: "github_events"
unique_keys: ["event_id"]
```

## Components

### 1. Extractor (`src/extractors/`)
- REST API client with pagination support
- Rate limiting and authentication
- Configurable request parameters

### 2. Validator (`src/validators/`)
- Pandera schema validation
- Data type checking and coercion
- Invalid record isolation

### 3. Transformer (`src/transformers/`)
- JSON normalization and flattening
- Type conversions with error handling
- Metadata enrichment

### 4. Loader (`src/loaders/`)
- PostgreSQL UPSERT operations
- Pipeline execution monitoring
- Transaction management

## Testing

```bash
# Setup virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Run tests
pytest -v
```

## Local Development

```bash
# Run individual components
python -c "from src.extractors.api_adapter import extract_api; extract_api('configs/sample_api.yaml', '2025-08-12')"
python -c "from src.validators.schema_validator import validate_schema; print(validate_schema('configs/sample_api.yaml', '2025-08-12'))"
python -c "from src.transformers.data_transformer import transform_data; df=transform_data('configs/sample_api.yaml', '2025-08-12'); print(len(df))"
```

## Monitoring

- **Pipeline Monitor Table**: Tracks execution metrics
- **Slack Notifications**: Configurable failure alerts
- **Streamlit Dashboard**: Real-time monitoring and manual triggers
- **Airflow UI**: DAG visualization and log inspection

## Data Flow

1. **Extract**: Fetch data from REST APIs → `data/raw/`
2. **Validate**: Schema validation → Valid data continues, invalid → `data/invalid/`
3. **Transform**: Type conversion and flattening → In-memory DataFrames
4. **Load**: UPSERT to PostgreSQL → Analytics tables

## Environment Variables

```bash
# Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Object Storage
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Airflow
AIRFLOW__CORE__FERNET_KEY=your-fernet-key
AIRFLOW_API_CONFIG=sample_api

# Notifications
SLACK_WEBHOOK_URL=your-slack-webhook
```

## Deployment

The pipeline is containerized and ready for:
- Local development with Docker Compose
- Kubernetes deployment (add K8s manifests)
- Cloud deployment (AWS ECS, GCP Cloud Run, etc.)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

MIT License - see LICENSE file for details.
