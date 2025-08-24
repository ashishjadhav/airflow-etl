# Standard library imports
from io import BytesIO

# Third party imports
from airflow.hooks.base import BaseHook
from minio import Minio


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
    print(f"Type of response parameter: {type(response)}")
    # Serialise and return the object
    return json.dumps(response.json()['chart']['result'][0])


def _store_prices(stock: dict) -> str:
    """ Store the data in a MinIO  database and returns the object path  """
    import json
    # Get Connection details via basehook
    minio = BaseHook.get_connection(conn_id="minio")

    # Create a Minio client
    # Add database endpoint url
    # Add username and password of the database
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    # Create the bucket if it doesn't exist
    bucket_name = 'stock-market'
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        
    # Store the stock prices object in the MinIO bucket
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf-8')  # Serialize into json string
    
    # Store the file as a binary buffer into the database
    objw = client.put_object(bucket_name=bucket_name,
                             object_name=f"{symbol}/prices.json",
                             data=BytesIO(data),
                             length=len(data)
                             )
    return f"{objw.bucket_name}/{symbol}"
