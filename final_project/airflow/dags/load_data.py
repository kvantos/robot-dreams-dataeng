import os
from airflow.decorators import dag
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from datetime import datetime
from google.cloud import storage


os.environ["no_proxy"] = "*"
BUCKET_NAME = 'robot-dreams-de2022-lesson10-data'
SRC_PREFIX = os.path.expanduser(
    '~/Projects/DE2022/lect_06/data_lake/landing/src1/sales'
    )
DST_PREFIX = 'src1/sales/v1'


@dag(
    schedule='0 1 * * *',
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 3),
    catchup=True,
    max_active_runs=1
    )
def load_data():
    @task
    def local2gcp(**kwargs):
        logical_run_date = kwargs['ds']
        src_path = f"{SRC_PREFIX}/{logical_run_date}"
        dst_path = f"{DST_PREFIX}/{'/'.join(logical_run_date.split('-'))}"
        gcs_client = storage.Client()
        bucket = gcs_client.bucket(BUCKET_NAME)
        files = os.listdir(src_path)

        if len(files) > 0:
            for file_name in files:
                blob = bucket.blob(f'{dst_path}/{file_name}')
                blob.upload_from_filename(f'{src_path}/{file_name}')
        else:
            raise AirflowFailException("No files for export")

    local2gcp()


dagg = load_data()
