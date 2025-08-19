import os
from airflow.models import Connection
from airflow import settings
from sqlalchemy.orm.exc import NoResultFound

def get_or_create_connection(conn_id, **kwargs):
    session = settings.Session()
    try:
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).one()
        print(f"Connection '{conn_id}' already exists.")
    except NoResultFound:
        conn = Connection(conn_id=conn_id, **kwargs)
        session.add(conn)
        session.commit()
        print(f"Connection '{conn_id}' created.")
    finally:
        session.close()

def main():
    # Postgres connection
    get_or_create_connection(
        'postgres_default',
        conn_type='postgres',
        host='postgres',
        login=os.environ.get('POSTGRES_USER', 'airflow'),
        password=os.environ.get('POSTGRES_PASSWORD', 'airflow'),
        schema='airflow',
        port=5432
    )
    # MinIO connection
    get_or_create_connection(
        'minio_default',
        conn_type='s3',
        host='http://minio:9000',
        login=os.environ.get('MINIO_ACCESS_KEY', os.environ.get('MINIO_ROOT_USER', 'minioadmin')),
        password=os.environ.get('MINIO_SECRET_KEY', os.environ.get('MINIO_ROOT_PASSWORD', 'minioadmin')),
        extra='{"aws_access_key_id": "%s", "aws_secret_access_key": "%s", "endpoint_url": "http://minio:9000"}' % (
            os.environ.get('MINIO_ACCESS_KEY', os.environ.get('MINIO_ROOT_USER', 'minioadmin')),
            os.environ.get('MINIO_SECRET_KEY', os.environ.get('MINIO_ROOT_PASSWORD', 'minioadmin'))
        )
    )

    # Slack connection (optional)
    slack_webhook = os.environ.get('SLACK_WEBHOOK_URL')
    if slack_webhook:
        get_or_create_connection(
            'slack_default',
            conn_type='http',
            host='slack.com',
            password=slack_webhook,
        )

if __name__ == "__main__":
    main()
