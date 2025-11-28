from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'equipe-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 16), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_etl_completo', 
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['etl', 'sentimento']
) as dag:

    task_bronze = BashOperator(
        task_id='extract_bronze',
        bash_command='cd /opt/airflow/dags/scripts/ && python bronze_script.py',
        dag=dag
    )

    task_silver = BashOperator(
        task_id='extract_silver',
        bash_command='cd /opt/airflow/dags/scripts/ && python silver_script.py',
        dag=dag
    )

    task_gold = BashOperator(
        task_id='extract_gold',
        bash_command='cd /opt/airflow/dags/scripts/ && python gold_script.py',
        dag=dag
    )

    task_load = BashOperator(
        task_id='load_database',
        bash_command='cd /opt/airflow/dags/scripts/ && python load_script.py',
        dag=dag
    )

    (task_bronze >> task_silver >> task_gold >> task_load)