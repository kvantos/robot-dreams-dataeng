
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
def process_customers():
    big_query = bigquery.Client(location='europe-west3')

    @task
    def customers_bronze2silver():
        proc_sql = """
            INSERT silver.customers (
                client_id,
                first_name,
                last_name,
                email,
                registration_date,
                state
                )
            SELECT DISTINCT
                cast(Id as INT64),
                FirstName,
                LastName,
                Email,
                cast(RegistrationDate as DATE),
                State
            FROM bronze.customers
        ;"""

        process_job = big_query.query(proc_sql,)
        data = process_job.result()
        print(data)

    customers_bronze2silver()


prcs_ctmrs = process_customers()
