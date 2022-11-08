
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
def nrch_users():
    big_query = bigquery.Client(location='europe-west3')

    @task
    def create_gold_table():
        create_sql = """
        CREATE OR REPLACE TABLE gold.user_profiles_enriched
        AS
        SELECT
            client_id,
            first_name,
            last_name,
            email,
            registration_date,
            state
        FROM silver.customers
        ;"""

        process_job = big_query.query(create_sql)
        data = process_job.result()
        print(data)

    @task
    def enrich_gold_table():
        proc_sql = """
        """
        process_job = big_query.query(proc_sql)
        data = process_job.result()
        print(data)
        
    create_gold_table() >> enrich_gold_table()


prcs_ctmrs = nrch_users()
