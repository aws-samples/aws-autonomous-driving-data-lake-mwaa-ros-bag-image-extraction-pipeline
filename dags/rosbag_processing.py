# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

from sensors.s3_metadata_sensor import S3MetadataSensor
from processing import processing


dag = DAG(
    dag_id='rosbag_processing',
    default_args={},
    schedule_interval='*/30 * * * *',
    dagrun_timeout=timedelta(minutes=240),
    start_date=days_ago(1),
    catchup=False,
    tags=['Industry Kit AV', 'AWS ProServe', 'ADAS']
)

bag_file_sensor = S3MetadataSensor(
    task_id='bag_file_sensor',
    bucket_key=f'*.bag',
    bucket_name=processing.get_parameter("/mwaa/rosbag/bag-src"),
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

tag_bag_file_in_process = PythonOperator(
    task_id='tag_bag_file_in_process',
    provide_context=True,
    op_kwargs={'tag_value': processing.METADATA_PROCESSING_VALUE_IN_PROGRESS},
    python_callable=processing.tag_bag,
    dag=dag,
)

tag_bag_file_failure = PythonOperator(
    task_id='tag_bag_file_failure',
    provide_context=True,
    op_kwargs={'tag_value': processing.METADATA_PROCESSING_VALUE_FAILURE},
    python_callable=processing.tag_bag,
    dag=dag,
)

tag_bag_file_complete = PythonOperator(
    task_id='tag_bag_file_complete',
    provide_context=True,
    op_kwargs={'tag_value': processing.METADATA_PROCESSING_VALUE_COMPLETE},
    python_callable=processing.tag_bag,
    dag=dag,
)

extract_png = PythonOperator(
    task_id='extract_png',
    provide_context=True,
    op_kwargs={
        'bucket_dest': processing.get_parameter("/mwaa/rosbag/bag-dest"),
        'fargate_cluster': processing.get_parameter("/mwaa/rosbag/fargate-cluster-arn"),
        'fargate_task_arn': processing.get_parameter("/mwaa/rosbag/task-definition-arn"),
        'fargate_task_name': processing.get_parameter("/mwaa/rosbag/task-definition-default-container-name"),
        'private_subnets': processing.get_parameter("/mwaa/rosbag/private-subnets")
    },
    python_callable=processing.run_fargate_task,
    dag=dag,
)

wait_for_extraction = BranchPythonOperator(
    task_id='wait_for_extraction',
    provide_context=True,
    op_kwargs={'fargate_cluster': processing.get_parameter("/mwaa/rosbag/fargate-cluster-arn")},
    python_callable=processing.wait_for_extraction,
    dag=dag,
)

extraction_success = DummyOperator(
    task_id='extraction_success',
    dag=dag,
)

extraction_failed = DummyOperator(
    task_id='extraction_failed',
    dag=dag,
)

label_images = PythonOperator(
    task_id='label_images',
    provide_context=True,
    python_callable=processing.label_images,
    op_kwargs={
        'bucket_dest': processing.get_parameter("/mwaa/rosbag/bag-dest"),
        'table_dest': processing.get_parameter("/mwaa/rosbag/dynamo-rek-results")
    },
    dag=dag,
)

draw_bounding_boxes = PythonOperator(
    task_id='draw_bounding_boxes',
    provide_context=True,
    python_callable=processing.draw_bounding_boxes,
    op_kwargs={
        'bucket_dest': processing.get_parameter("/mwaa/rosbag/bag-dest")
    },
    dag=dag,
)

no_work = DummyOperator(
    task_id='no_work',
    dag=dag,
)

bag_file_sensor >> determine_work >> [no_work, tag_bag_file_in_process]

tag_bag_file_in_process >> extract_png >> wait_for_extraction >> [extraction_success, extraction_failed]

extraction_success >> label_images >> draw_bounding_boxes >> tag_bag_file_complete
extraction_failed >> tag_bag_file_failure

if __name__ == "__main__":
    dag.cli()
