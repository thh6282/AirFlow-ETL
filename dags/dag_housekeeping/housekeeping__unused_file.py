from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


from dags.utils.clean_metadata import housekeeping__clean_unused_file



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'email': ['anhnh@raffles.com.vn'],
    'email_on_failure': True,
    'email_on_retry': True
}

dag_housekeeping__unused_file = DAG(
    'housekeeping__unused_file',
    default_args=default_args,
    description='A DAG to clean unused files in minio',
    start_date=datetime(2024, 8, 1),
    schedule_interval='0 6 * * *',
    tags=['housekeeping'],
    catchup=False,
    dagrun_timeout=timedelta(minutes=180)
)

# 0 12 L * *
start = DummyOperator(task_id='start', dag=dag_housekeeping__unused_file)
end = DummyOperator(task_id='end', trigger_rule='all_done', dag=dag_housekeeping__unused_file)


run_housekeeping__unused_file = PythonOperator(
    task_id='housekeeping__clean_unused_file',
    python_callable=housekeeping__clean_unused_file,
    dag=dag_housekeeping__unused_file,
    trigger_rule='all_done'
)

start >> run_housekeeping__unused_file >> end
