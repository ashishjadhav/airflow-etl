from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue 
from airflow.operators.python import PythonOperator


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["stock_market"],
)

def stock_market():
    
    # Define the tasks of the DAG
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available()->PokeReturnValue:
        """Logic to check if the stock API is available"""
        import requests
        api = BaseHook.get_connection("stock_api")
        url =f"{api.host}{api.extra_dejson['endpoint']}"
        print(f"Checking API availability at {url}")
        # Make GET request to the Weather API
        response = requests.get(url, headers=api.extra_dejson['headers'])

        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices
    )

    is_api_available()


# Create the DAG
stock_market()