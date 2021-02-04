import os

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
# from sensors.s3_metadata_sensor import S3MetadataSensor


def print_env_vars():
    for key in os.environ.keys():
        print(f"{key} -> {os.environ[key]}")

args = {
    'owner': 'hschoen@amazon.com'
}

dag = DAG(
    dag_id='bag_processing',
    default_args=args,
    schedule_interval='*/10 * * * *',
    dagrun_timeout=timedelta(minutes=60),
    start_date=days_ago(1),
    catchup=False,
    tags=['Industry Kit AV']
)

get_env_vars = PythonOperator(
    task_id='get_env_vars',
    python_callable=print_env_vars
)

# check_new_bag_file = S3MetadataSensor(
#     task_id='check_new_bag_file',
#     bucket_key=f'*.bag',
#     bucket_name=SOURCE_BUCKET,
#     wildcard_match=True,
#     metadata_key=METADATA_PROCESSING_KEY,
#     metadata_values=[METADATA_PROCESSING_VALUE_FAILURE, METADATA_PROCESSING_VALUE_COMPLETE,
#                      METADATA_PROCESSING_VALUE_IN_PROGRESS, METADATA_PROCESSING_VALUE_CANCELLED],
#     poke_interval=10,
#     timeout=30,
#     aws_conn_id='aws_default',
#     soft_fail=True,
#     dag=dag,
# )

extract_png = DummyOperator(
    task_id='extract_png',
    dag=dag,
)

label_images = DummyOperator(
    task_id='label_images',
    dag=dag,
)

get_env_vars >>  extract_png >> label_images

if __name__ == "__main__":
    dag.cli()
