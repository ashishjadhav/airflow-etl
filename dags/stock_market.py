# Standard library imports
from datetime import datetime

# Third party imports
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.base import PokeReturnValue

# Custom imports
from include.stock_market.tasks import _get_stock_prices, _store_prices

SYMBOL = "NVDA"

# Define the basic parameters of the DAG, like schedule and start_date


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["stock_market"],
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
    # Task 4 
    # get_formatted_csv = PythonOperator(
    #     task_id='get_formatted_csv',
    #     python_callable=_get_formatted_csv,
    #     op_kwargs={
    #         'input_data': '{{ ti.xcom_pull(task_ids="store_prices") }}'
    #     }
    # )


# Set sequence of the tasks
    is_api_available() >> get_stock_prices >> store_prices >> format_prices


# Create the DAG
stock_market()
