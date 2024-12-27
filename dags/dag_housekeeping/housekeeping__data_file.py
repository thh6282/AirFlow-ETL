from datetime import datetime, timedelta
from dags.utils.etl_manager import AirflowETLManager
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from dags.utils.clean_data import housekeeping__clean_data_file

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['anhnh@raffles.com.vn'],
    'email_on_failure': True,
    'email_on_retry': True
}

dag_housekeeping__data_files = DAG(
    'housekeeping__data_files',
    default_args=default_args,
    description='DAG to optimized data iceberg tables',
    start_date=datetime(2024, 8, 1),
    schedule_interval='0 12 1 * *',
    tags=['housekeeping'],
    catchup=False,
    dagrun_timeout=timedelta(minutes=360)
)

etl_manager = AirflowETLManager()
start = DummyOperator(task_id='start', dag=dag_housekeeping__data_files)
    
run_housekeeping__clean_data_file = PythonOperator(
    task_id='housekeeping__data_files',
    python_callable=housekeeping__clean_data_file,
    dag=dag_housekeeping__data_files,
    trigger_rule='all_done'
)

end = PythonOperator(
    task_id='end',
    python_callable=etl_manager.task_end,
    op_args=['housekeeping__data_files', 'housekeeping'],
    provide_context=True,
    trigger_rule='all_done',
    dag=dag_housekeeping__data_files,
)


start >> run_housekeeping__clean_data_file >> end