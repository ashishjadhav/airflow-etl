# Standard library imports
from datetime import datetime

# Third party imports
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack_notifier import SlackNotifier
from airflow.sensors.base import PokeReturnValue
from astro import sql as aql
from astro.files import File
from astro.sql.table import Metadata, Table

# Custom imports
from include.stock_market.tasks import (
    BUCKET_NAME,
    _get_formatted_csv,
    _get_stock_prices,
    _store_prices,
)

SYMBOL = "NVDA"

# Define the basic parameters of the DAG, like schedule and start_date


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["stock_market"],
    on_success_callback=SlackNotifier(slack_conn_id='slack',
                                      text="DAG Succeeded",
                                      channel="#general"),

    on_failure_callback=SlackNotifier(slack_conn_id='slack',
                                      text="DAG Failed",
                                      channel="#general")
)
def stock_market():

    # Define the tasks of the DAG
    # Task 1
    @task.sensor(task_id='is_api_available', poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        """Logic to check if the stock API is available"""
        import requests

        # Access the connection data of the API via BaseHook
        api = BaseHook.get_connection(conn_id="stock_api")
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(f"Checking API availability at {url}")

        # Make GET request to the Weather API
        response = requests.get(url, headers=api.extra_dejson['headers'])
        print(f"Content of the response, in unicode : {response.text}")
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    # Task 2
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',  # Unique Task name
        python_callable=_get_stock_prices,
        op_kwargs={
            'url': '{{ ti.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL},  # Keyword arguments of the function
    )
    # Task 3
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={
            'stock': '{{ ti.xcom_pull(task_ids="get_stock_prices") }}'},
    )

    # Task 4
    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove='success',
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        }
    )
    # Task 5
    get_formatted_csv = PythonOperator(
        task_id='get_formatted_csv',
        python_callable=_get_formatted_csv,
        op_kwargs={
            'path': '{{ ti.xcom_pull(task_ids="store_prices") }}'
        }
    )
    # Task 5
    load_to_dw = aql.load_file(
        task_id='load_to_dw',
        input_file=File(
            path=f"s3://{BUCKET_NAME}/{{{{ti.xcom_pull(task_ids='get_formatted_csv')}}}}",
            conn_id='minio'
        ),
        output_table=Table(
            name='stock_market',
            conn_id='postgres',
            metadata=Metadata(
                schema='public',
            )
        ),
        load_options={
            "aws_access_key_id": BaseHook.get_connection('minio').login,
            "aws_secret_access_key": BaseHook.get_connection('minio').password,
            "endpoint_url": BaseHook.get_connection('minio').host,
        }

    )

# Set sequence of the tasks
    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_csv >> load_to_dw


# Create the DAG
stock_market()
