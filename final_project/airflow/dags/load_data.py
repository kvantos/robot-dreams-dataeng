import os
from airflow.decorators import dag
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery


os.environ["no_proxy"] = "*"
BUCKET_NAME = 'de-2022-incoming-data-raw'


@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    end_date=None,
    # catchup=True,
    max_active_runs=1
    )
def load_sales():
    big_query = bigquery.Client(location='europe-west3')
    gcs_client = storage.Client()

    @task
    def load_sales2bronze():
        # logical_run_date = kwargs['ds']
        bkt = gcs_client.bucket(BUCKET_NAME)
        xtrnl_cfg = bigquery.ExternalConfig("CSV")
        xtrnl_cfg.skip_leading_rows = 1
        xtrnl_cfg.schema = [
            bigquery.SchemaField("CustomerId", "STRING"),
            bigquery.SchemaField("PurchaseDate", "STRING"),
            bigquery.SchemaField("Product", "STRING"),
            bigquery.SchemaField("Price", "STRING"),
        ]

        xtrnl_cfg.source_uris = [
            os.path.join('gs://', BUCKET_NAME, b.name) for b in bkt.list_blobs(prefix='sales')
        ]

        job_cfg = bigquery.QueryJobConfig(table_definitions={'sales_raw': xtrnl_cfg})

        load_sql = """
            INSERT bronze.sales (
                CustomerId,
                PurchaseDate,
                Product,
                Price
                )
            SELECT
                CustomerId,
                PurchaseDate,
                Product,
                Price
            FROM sales_raw
        ;"""

        if len(xtrnl_cfg.source_uris) > 0:
            query_job = big_query.query(load_sql, job_config=job_cfg)
            data = query_job.result()
            print(data)
        else:
            raise AirflowFailException("No files for load")

    load_sales2bronze()


dagg = load_sales()
