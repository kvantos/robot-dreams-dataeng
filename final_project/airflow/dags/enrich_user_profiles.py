
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
def enrich_user_profiles():
    big_query = bigquery.Client(location='europe-west3')

    @task
    def create_gold_table():
        create_sql = """
        CREATE OR REPLACE TABLE gold.user_profiles_enriched
        AS
        SELECT
          c.client_id,
          c.email,
          split(up.full_name, ' ')[OFFSET(0)] as first_name,
          split(up.full_name, ' ')[OFFSET(1)] as last_name,
          up.full_name,
          up.birth_date,
          up.phone_number,
          up.state,
          c.registration_date
        FROM silver.customers c
        LEFT JOIN silver.user_profiles up
        ON c.email = up.email
        ;"""

        process_job = big_query.query(create_sql)
        data = process_job.result()
        print(data)

    create_gold_table()


prcs_ctmrs = enrich_user_profiles()
