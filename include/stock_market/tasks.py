# Standard library imports
from io import BytesIO

from airflow.exceptions import AirflowNotFoundException

# Third party imports
from airflow.hooks.base import BaseHook
from minio import Minio

BUCKET_NAME = 'stock-market'


def _get_minio_client() -> Minio:
    """Create and return a Minio client."""

    # Get Connection details via basehook
    minio = BaseHook.get_connection(conn_id="minio")

    # Create a Minio client
    # Give database endpoint url
    # Give username and password of the database
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client


def _get_stock_prices(url: str, symbol: str) -> str:
    """
    Fetch stock prices from the API.
    """
    import json

    import requests

    url = f"{url}{symbol}?metrics=high&interval=1d&range=1y"

    # Get Connection details via basehook
    api = BaseHook.get_connection(conn_id="stock_api")

    # Get requests.Response object
    response = requests.get(url, headers=api.extra_dejson['headers'])

    # Serialise and return the json formatted string
    print(
        f"OP Type is: {type(json.dumps(response.json()['chart']['result'][0]))}")
    return json.dumps(response.json()['chart']['result'][0])


def _store_prices(stock: str) -> str:
    """ Store the data in a MinIO  database and returns the path of the location of the object """
    import json

    client = _get_minio_client()

    # Create the bucket if it doesn't exist
    bucket_name = BUCKET_NAME
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Store the stock prices object in the MinIO bucket
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode(
        'utf-8')  # Serialize into json string

    # Store the file as a binary buffer into the database
    objw = client.put_object(bucket_name=bucket_name,
                             object_name=f"{symbol}/prices.json",
                             data=BytesIO(data),
                             length=len(data)
                             )
    return f"{objw.bucket_name}/{symbol}"


def _get_formatted_csv(path: str)-> str:
    client = _get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(
        bucket_name=BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return f"{obj.object_name}"
    return AirflowNotFoundException("CSV file does not exist")
