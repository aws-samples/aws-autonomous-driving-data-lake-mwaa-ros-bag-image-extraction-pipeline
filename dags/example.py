from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

args = {
    'owner': 'example@email.com'
}

dag = DAG(
    dag_id='example_dag',
    default_args=args,
    schedule_interval='*/10 * * * *',
    dagrun_timeout=timedelta(minutes=60),
    start_date=days_ago(7),
    catchup=False,
    tags=['example']
)

step_one = DummyOperator(
    task_id='step_one',
    dag=dag,
)

step_two = DummyOperator(
    task_id='step_two',
    dag=dag,
)

step_three = DummyOperator(
    task_id='step_three',
    dag=dag,
)

step_one >> step_two >> step_three

if __name__ == "__main__":
    dag.cli()
