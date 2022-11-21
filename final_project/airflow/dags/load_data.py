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
    max_active_runs=1
    )
def load_sales():
    big_query = bigquery.Client(location='europe-west3')
    gcs_client = storage.Client()

    @task
    def load_sales2bronze():
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

        clean_sql = """
        DELETE FROM bronze.sales
        WHERE CustomerId = 'CustomerId'
        ;"""

        delete_job = big_query.query(clean_sql)
        data = delete_job.result()
        print(data)

    load_sales2bronze()


@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    end_date=None,
    max_active_runs=1
    )
def load_customers():
    big_query = bigquery.Client(location='europe-west3')
    gcs_client = storage.Client()

    @task
    def load_customers2bronze():
        bkt = gcs_client.bucket(BUCKET_NAME)
        xtrnl_cfg = bigquery.ExternalConfig("CSV")
        xtrnl_cfg.skip_leading_rows = 1
        xtrnl_cfg.schema = [
            bigquery.SchemaField("Id", "STRING"),
            bigquery.SchemaField("FirstName", "STRING"),
            bigquery.SchemaField("LastName", "STRING"),
            bigquery.SchemaField("Email", "STRING"),
            bigquery.SchemaField("RegistrationDate", "STRING"),
            bigquery.SchemaField("State", "STRING"),
        ]

        xtrnl_cfg.source_uris = [
            os.path.join('gs://', BUCKET_NAME, b.name) for b in bkt.list_blobs(prefix='customers')
        ]

        job_cfg = bigquery.QueryJobConfig(table_definitions={'customers_raw': xtrnl_cfg})

        load_sql = """
            INSERT bronze.customers (
                Id,
                FirstName,
                LastName,
                Email,
                RegistrationDate,
                State
                )
            SELECT DISTINCT
                Id,
                FirstName,
                LastName,
                Email,
                RegistrationDate,
                State
            FROM customers_raw
        ;"""

        if len(xtrnl_cfg.source_uris) > 0:
            query_job = big_query.query(load_sql, job_config=job_cfg)
            data = query_job.result()
            print(data)
        else:
            raise AirflowFailException("No files for load")

        clean_sql = """
        DELETE FROM bronze.customers
        WHERE Id = 'Id'
        ;"""

        clean_job = big_query.query(clean_sql)
        data = clean_job.result()
        print(data)

    load_customers2bronze()


@dag(
    schedule=None,
    start_date=datetime(2021, 1, 1),
    end_date=None,
    max_active_runs=1
    )
def load_usr_prfls():
    big_query = bigquery.Client(location='europe-west3')
    gcs_client = storage.Client()

    @task
    def load_profiles2bronze():
        bkt = gcs_client.bucket(BUCKET_NAME)
        xtrnl_cfg = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
        xtrnl_cfg.schema = [
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("full_name", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("birth_date", "STRING"),
            bigquery.SchemaField("phone_number", "STRING")
        ]

        xtrnl_cfg.source_uris = [
            os.path.join('gs://', BUCKET_NAME, b.name) for b in bkt.list_blobs(prefix='user_profiles')
        ]

        job_cfg = bigquery.QueryJobConfig(table_definitions={'profiles_raw': xtrnl_cfg})

        load_sql = """
            INSERT bronze.user_profiles (
                email,
                full_name,
                state,
                birth_date,
                phone_number
                )
            SELECT DISTINCT
                email,
                full_name,
                state,
                birth_date,
                phone_number
            FROM profiles_raw
        ;"""

        if len(xtrnl_cfg.source_uris) > 0:
            query_job = big_query.query(load_sql, job_config=job_cfg)
            data = query_job.result()
            print(data)
        else:
            raise AirflowFailException("No files for load")

    load_profiles2bronze()


dagg = load_sales()
dag2 = load_customers()
dag3 = load_usr_prfls()
