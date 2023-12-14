from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.alert import price_alert

with DAG(
    dag_id="dag_alert",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule_interval="0 0 * * *",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
) as dag:

    insert_into_db_task = PythonOperator(
        task_id="insert_into_db",
        python_callable=price_alert,
        op_kwargs={
            "config_file": "/opt/airflow/config/config.ini",
            "whitelist": [
                {
                    'coin_id': 1,
                    'ticker': 'ETHBTC',
                    'target_price': 0.0000001
                },
                {
                    'coin_id': 2,
                    'ticker': 'LTCBTC',
                    'target_price': 0.000001
                }
            ]
        }
    )
