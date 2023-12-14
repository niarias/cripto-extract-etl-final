from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.binance import get_binance_pairs, get_24hr_ticker
# Importar DummyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.main import insert_into_db


with DAG(
    dag_id="extract_dag",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule_interval="0 0 * * *",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
) as dag:

    dummy_start_task = DummyOperator(
        task_id="dummy_start"
    )

    create_tables_task = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="redshift_conn_1",
        sql="sql/creates.sql",
        hook_params={
            "options": "-c search_path=nicolas_ezequiel_arias300_coderhouse"
        }
    )

    insert_into_db_task = PythonOperator(
        task_id="insert_into_db",
        python_callable=insert_into_db,
        op_kwargs={
            "config_file": "/opt/airflow/config/config.ini",
            "whitelist": [
                {
                    'coin_id': 1,
                    'ticker': 'ETHBTC',
                },
                {
                    'coin_id': 2,
                    'ticker': 'LTCBTC',
                }
            ]
        }
    )

    dummy_end_task = DummyOperator(
        task_id="dummy_end_task"
    )

    dummy_start_task >> create_tables_task >> insert_into_db_task >> dummy_end_task
