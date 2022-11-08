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
def process_usr_profiles():
    big_query = bigquery.Client(location='europe-west3')

    @task
    def process_bronze2silver():
        process_sql = """
            INSERT silver.user_profiles (
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
                cast(birth_date as DATE),
                phone_number
            FROM bronze.user_profiles
        ;"""

        query_job = big_query.query(process_sql)
        data = query_job.result()
        print(data)

    process_bronze2silver()


prcs_usr_prfls = process_usr_profiles()
