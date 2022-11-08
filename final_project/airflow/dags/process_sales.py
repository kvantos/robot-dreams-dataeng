import os
from airflow.decorators import dag
from airflow.decorators import task
from datetime import datetime
from google.cloud import bigquery


os.environ["no_proxy"] = "*"


@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    end_date=None,
    max_active_runs=1
    )
def process_sales():
    big_query = bigquery.Client(location='europe-west3')

    @task
    def process_bronze2silver():
        process_sql = """
            INSERT silver.sales (
                client_id,
                purchase_date,
                product_name,
                price
                )
            SELECT
                cast(CustomerId as INT64),
                cast(REPLACE(PurchaseDate, '/', '-') as DATE),
                Product,
                CAST(TRIM(Price, '$') AS INT64)
            FROM bronze.sales
        ;"""

        query_job = big_query.query(process_sql)
        data = query_job.result()
        print(data)

    process_bronze2silver()


prcs_sales = process_sales()
