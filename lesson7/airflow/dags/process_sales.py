from airflow.decorators import dag
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from datetime import datetime
import requests
import os

os.environ["no_proxy"] = "*"
GET_API_URL = 'http://127.0.0.1:8081/apiworker'
CONVERT_URL = 'http://127.0.0.1:8081/storageworker'


@dag(
    schedule='0 1 * * *',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 12),
    catchup=True,
    max_active_runs=1
    )
def process_sales():
    @task
    def extract_data_from_api(**kwargs):
        logical_run_date = kwargs['ds']
        req_json = {
            "date": logical_run_date,
            "raw_dir": f"raw/sales/{logical_run_date}"
            }
        rpl = requests.post(GET_API_URL, json=req_json)
        if rpl.status_code != 201:
            error_message = f'API error {rpl.status_code} {rpl.text}'
            raise AirflowFailException(error_message)

        return logical_run_date

    @task
    def convert_to_avro(run_date):
        req_json = {
            "raw_dir": f"raw/sales/{run_date}",
            "stg_dir": f"stg/sales/{run_date}"
            }

        rpl = requests.post(CONVERT_URL, json=req_json)
        if rpl.status_code != 201:
            error_message = f'Convertor error {rpl.status_code} {rpl.text}'
            raise AirflowFailException(error_message)

    gdata = extract_data_from_api()
    ctoavro = convert_to_avro(gdata)
    gdata >> ctoavro


dagg = process_sales()
