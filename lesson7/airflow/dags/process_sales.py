from airflow.decorators import dag, task
from datetime import datetime

import requests
# import logging


# raw_dir та stg_dir
@dag(
    schedule_interval='0 1 * * *',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 11)
    catchup=True,
    max_active_runs=1
    )
def process_sales():
    @task(task_id='extract_data_from_api')
    def get_data() -> int:
        print("ho")
        return 201

    @task(task_id='convert_to_avro')
    def process_data(status_code: int):
        print("haha")

    process_data(get_data())


dagg = process_sales()
