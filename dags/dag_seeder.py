from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.seed import seedCoins, seedExchanges


with DAG(
    dag_id="seed_initial_data",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule_interval="0 0 * * *"
) as dag:
    dummy_start_task = DummyOperator(
        task_id="dummy_seed_start"
    )

    create_tables_task = PostgresOperator(
        task_id="create_seed_tables",
        postgres_conn_id="redshift_conn_1",
        sql="sql/seeder.sql",
        hook_params={
            "options": "-c search_path=nicolas_ezequiel_arias300_coderhouse"
        }
    )

    seed_coins_task = PythonOperator(
        task_id="insert_coins_into_db",
        python_callable=seedCoins,
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

    seed_exchanges_task = PythonOperator(
        task_id="insert_exchanges_into_db",
        python_callable=seedExchanges,
        op_kwargs={
            "config_file": "/opt/airflow/config/config.ini",
            "whitelist": [
                {
                    'exchange_id': 1,
                    'name': 'BINANCE',
                    'url': 'https://binance.com',
                }
            ]
        }
    )

    dummy_end_task = DummyOperator(
        task_id="dummy_seed_end_task"
    )

    dummy_start_task >> create_tables_task
    create_tables_task >> seed_coins_task
    create_tables_task >> seed_exchanges_task
    seed_coins_task >> dummy_end_task
    seed_exchanges_task >> dummy_end_task
