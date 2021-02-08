import os

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from sensors.s3_metadata_sensor import S3MetadataSensor
from processing import processing


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
    tags=['Industry Kit AV', 'AWS ProServe', 'ADAS']
)

get_env_vars = PythonOperator(
    task_id='get_env_vars',
    python_callable=print_env_vars
)

bag_file_sensor = S3MetadataSensor(
    task_id='bag_file_sensor',
    bucket_key=f'*.bag',
    bucket_name=os.environ["AIRFLOW__BAG__SRC"],
    wildcard_match=True,
    metadata_key=processing.METADATA_PROCESSING_KEY,
    metadata_values=[processing.METADATA_PROCESSING_VALUE_FAILURE,
                     processing.METADATA_PROCESSING_VALUE_COMPLETE,
                     processing.METADATA_PROCESSING_VALUE_IN_PROGRESS],
    poke_interval=10,
    timeout=30,
    aws_conn_id='aws_default',
    soft_fail=True,
    dag=dag,
)

determine_work = BranchPythonOperator(
    task_id=f'determine_work',
    python_callable=processing.determine_workload,
    provide_context=True,
    dag=dag,
)

tag_bag_file = PythonOperator(
    task_id='tag_bag_file',
    provide_context=True,
    python_callable=processing.tag_bag,
    dag=dag,
)

extract_png = PythonOperator(
    task_id='extract_png',
    provide_context=True,
    op_kwargs={
        'bucket_dest': os.environ["AIRFLOW__BAG__DEST"],
        'fargate_cluster': os.environ["AIRFLOW__FARGATE__CLUSTER"],
        'fargate_task_arn': os.environ["AIRFLOW__FARGATE__TASK_ARN"],
        'fargate_task_name': os.environ["AIRFLOW__FARGATE__TASK_NAME"],
        'private_subnets': os.environ['AIRFLOW__PRIVATE__SUBNETS']
    },
    python_callable=processing.run_fargate_task,
    dag=dag,
)

label_images = DummyOperator(
    task_id='label_images',
    dag=dag,
)

no_work = DummyOperator(
    task_id='no_work',
    dag=dag,
)

get_env_vars >> bag_file_sensor >> determine_work >> [no_work, tag_bag_file]

tag_bag_file >> extract_png >> label_images


if __name__ == "__main__":
    dag.cli()
