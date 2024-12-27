from datetime import datetime, timedelta
from dags.utils.etl_manager import AirflowETLManager
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from dags.utils.clean_metadata import housekeeping__clean_metadata_file, check_status_housekeeping_data_files

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['anhnh@raffles.com.vn'],
    'email_on_failure': True,
    'email_on_retry': True
}

dag_housekeeping__metadata_files = DAG(
    'housekeeping__metadata_files',
    default_args=default_args,
    description='DAG to clean metadata files',
    start_date=datetime(2024, 8, 1),
    schedule_interval='0 12 L * *',
    tags=['housekeeping'],
    catchup=False,
    dagrun_timeout=timedelta(minutes=360)
)

etl_manager = AirflowETLManager()

run_check_status_housekeeping_data_file = BranchPythonOperator(
        task_id='check_status_housekeeping_data_file',
        python_callable=check_status_housekeeping_data_files,
        dag=dag_housekeeping__metadata_files,
        provide_context=True, 
    )

run_housekeeping__clean_data_file = PythonOperator(
    task_id='housekeeping__metadata_files',
    python_callable=housekeeping__clean_metadata_file,
    dag=dag_housekeeping__metadata_files,
    trigger_rule='all_done'
)

end = PythonOperator(
    task_id='end',
    python_callable=etl_manager.task_end,
    op_args=['housekeeping__metadata_files', 'housekeeping'],
    provide_context=True,
    trigger_rule='all_done',
    dag=dag_housekeeping__metadata_files,
)


run_check_status_housekeeping_data_file >> run_housekeeping__clean_data_file >> end
run_check_status_housekeeping_data_file >> end