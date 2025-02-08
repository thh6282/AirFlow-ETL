from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from dags.utils.etl_manager import AirflowETLManager
from dags.utils.send_email import check_schema_change, get_recipient_email_list_schema_change, get_content_email_schema_change, update_email_notification_status
from airflow.utils.edgemodifier import Label
from airflow.models import Variable
from dags.utils.variables import staging_threads, ETL_DATE, k8s_namespace, dbt_image, SOURCES_DICT1, default_args_config_dags



PROFILES_DIR = "/dbt"
PROJECT_DIR = "/dbt"

METADATA_PATH = "models/operational_metadata"

STAGING_PATH_EXACT = "models/staging/exact"
STAGING_PATH_EXACT101 = "models/staging/exact101"
STAGING_PATH_EXACT500 = "models/staging/exact500"
STAGING_PATH_EXACT600 = "models/staging/exact600"

STAGING_PATH_NHAMAY1 = "models/staging/nhamay"
STAGING_PATH_NHAMAY2 = "models/staging/nhamay210_220_668"

STAGING_PATH_APPDATA_SHG_BOOKING = "models/staging/appdatabooking"
STAGING_PATH_APPDATA_SHG_EXTAPPDATA = "models/staging/appdataext"


def create_staging_dag(source_name:str, data_source_name:str, default_args, prefix_source:str, dag_id: str) -> DAG:
    """Function to create a DAG staging for each source"""
    # Define Manager
    etl_manager = AirflowETLManager()

    # Define bash command for dbt run operational_metadata and staging for all sources
    if source_name.startswith("exact"):
        if source_name == "exact500":
            bash_command_metadata = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}'}}" -s path:{METADATA_PATH}/*"""
            bash_command_staging = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}' }}" -s path:{STAGING_PATH_EXACT500}/*"""
        elif source_name == "exact600":
            bash_command_metadata = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}'}}" -s path:{METADATA_PATH}/*"""
            bash_command_staging = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}'}}" -s path:{STAGING_PATH_EXACT600}/*"""
        elif source_name == "exact101":
            bash_command_metadata = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}'}}" -s path:{METADATA_PATH}/*"""
            bash_command_staging = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}'}}" -s path:{STAGING_PATH_EXACT101}/*"""
        else:
            bash_command_metadata = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'source_name_exact': {source_name}, 'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}' }}" -s path:{METADATA_PATH}/*"""
            bash_command_staging = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'source_name_exact': {source_name}, 'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}'}}" -s path:{STAGING_PATH_EXACT}/*"""
    elif source_name.startswith("appdata"):
        if source_name == "appdatashgextappdata":
            bash_command_metadata = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'source_name_appdata': {source_name}, 'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}'}}" -s path:{METADATA_PATH}/*"""
            bash_command_staging = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{ 'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}'}}" -s path:{STAGING_PATH_APPDATA_SHG_EXTAPPDATA}/*"""
        elif source_name == "appdatashgbooking":
            bash_command_metadata = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}' }}" -s path:{METADATA_PATH}/*"""
            bash_command_staging = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{ 'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}'}}" -s path:{STAGING_PATH_APPDATA_SHG_BOOKING}/*"""
    elif source_name.startswith("nhamay"):
        if source_name == "nhamay210" or source_name == "nhamay220" or source_name == "nhamay668": 
            bash_command_metadata = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'source_name_nhamay2': {source_name}, 'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}', 'prefix_sources_nhamay2': '{prefix_source}' }}" -s path:{METADATA_PATH}/*"""
            bash_command_staging = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'source_name_nhamay2': {source_name}, 'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}', 'prefix_sources_nhamay2': '{prefix_source}'}}" -s path:{STAGING_PATH_NHAMAY2}/*"""
        else:
            bash_command_metadata = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'source_name_nhamay': {source_name}, 'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}', 'prefix_sources': '{prefix_source}'}}" -s path:{METADATA_PATH}/*"""
            bash_command_staging = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars "{{'source_name_nhamay': {source_name}, 'data_source_name': {data_source_name}, 'source_name': {source_name}, 'etl_date': '{ETL_DATE}', 'prefix_sources': '{prefix_source}'}}" -s path:{STAGING_PATH_NHAMAY1}/*"""

    with DAG(
        dag_id,
        default_args=default_args,
        description=f'A DAG to run dbt models for {source_name} in the staging',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        tags=['etl_pipeline', source_name, 'staging'],
        catchup=False,
        dagrun_timeout=timedelta(minutes=240)
    ) as dag_staging:

        start = PythonOperator(
            task_id='start',
            python_callable=etl_manager.fetch_run_id,
            op_args=[f'{source_name}', f'{dag_id}'],
            retries=5,
            retry_delay=timedelta(seconds=10),
            provide_context=True,
        )

        end = PythonOperator(
            task_id='end',
            python_callable=etl_manager.task_end,
            op_args=[dag_id, f'{source_name}'],
            provide_context=True,
            trigger_rule='all_done',
        )

        run_operational_metadata = KubernetesPodOperator(
            kubernetes_conn_id='k8s_cluster',
            task_id='operational_metadata',
            name='operational_metadata',
            namespace=k8s_namespace,
            image=dbt_image,
            cmds=["/bin/bash", "-c", bash_command_metadata],
            get_logs=True,
            is_delete_operator_pod=True,
            in_cluster=False,
            startup_timeout_seconds=7200,
            env_vars={
                'DBT_THREADS': str(staging_threads),
                '_RUN_ID': "{{ task_instance.xcom_pull(task_ids='start', key='_run_id__' ~ params.source_name) }}",
                '_USER_TRINO': str(etl_manager.user_dbt),
                '_PASSWORD_TRINO': str(etl_manager.password_dbt),
                '_DATABASE_TRINO': str(etl_manager.database_dbt),
                '_HOST_TRINO': str(etl_manager.host_dbt),
                '_PORT_TRINO': str(etl_manager.port_dbt)
            },
            params={
                'source_name': source_name  
            },
            trigger_rule='none_skipped',
            retries=5,
            retry_delay=timedelta(seconds=60),
        )
      
        check_schema_drift = BranchPythonOperator(
            task_id='check_schema_drift_task',
            python_callable=check_schema_change,
            provide_context=True,
            op_kwargs={'source_name': source_name}
        )

        with TaskGroup('schema_drift_notification') as schema_drift_notification:
           
            send_email_task = EmailOperator(
                task_id='send_email_task_schema_drift',
                to="{{ task_instance.xcom_pull(task_ids='schema_drift_notification.recipient_email_list_schema_drift') | join(',')}}",
                subject=f'Schema change report for the source {source_name}  on {ETL_DATE}',
                html_content="{{ task_instance.xcom_pull(task_ids='schema_drift_notification.generate_content_email_schema_drift') }}",
                conn_id='k8s_email'
            )
            
            # task to update email notification status table
            update_email_notification_task = PythonOperator(
                task_id='update_email_notification_task',
                python_callable=update_email_notification_status,
                op_kwargs={'source_name': source_name}
            )

            # Tasks to get email list and content
            get_recipient_email_list_task = PythonOperator(
                task_id='recipient_email_list_schema_drift',
                python_callable=get_recipient_email_list_schema_change,
                do_xcom_push=True,
                op_kwargs={'source_name': source_name}
            )

            generate_content_email_task = PythonOperator(
                task_id='generate_content_email_schema_drift',
                python_callable=get_content_email_schema_change,
                do_xcom_push=True,
                op_kwargs={'source_name': source_name}
            )

            get_recipient_email_list_task >> generate_content_email_task >> send_email_task >> update_email_notification_task

        run_staging = KubernetesPodOperator(
            kubernetes_conn_id='k8s_cluster',
            task_id='staging',
            name='staging',
            namespace=k8s_namespace,
            image=dbt_image,
            cmds=["/bin/bash", "-c", bash_command_staging],
            get_logs=True,
            is_delete_operator_pod=True,
            in_cluster=False,
            startup_timeout_seconds=7200,
            env_vars={
                'DBT_THREADS': str(staging_threads),
                '_RUN_ID': "{{ task_instance.xcom_pull(task_ids='start', key='_run_id__' ~ params.source_name) }}",
                '_USER_TRINO': str(etl_manager.user_dbt),
                '_PASSWORD_TRINO': str(etl_manager.password_dbt),
                '_DATABASE_TRINO': str(etl_manager.database_dbt),
                '_HOST_TRINO': str(etl_manager.host_dbt),
                '_PORT_TRINO': str(etl_manager.port_dbt)
            },
            params={
                'source_name': source_name  
            },
            trigger_rule='all_done',
            retries=5,
            retry_delay=timedelta(seconds=60),
        )

        run_operational_metadata >> check_schema_drift >> Label("Nguồn thay đổi cấu trúc") >> schema_drift_notification >> end
        run_operational_metadata >> check_schema_drift >> Label("Nguồn không thay đổi cấu trúc") >> end
        start >> run_operational_metadata >> run_staging >> end
        

    return dag_staging


# Tạo nhiều DAG cho nhiều staging


for source_name, values in SOURCES_DICT1.items():

    source_name = source_name
    data_source_name =  values["data_source"]
    prefix_source = values["prefix"]
    num_staging = values["num_staging"]

    dag_id = f'{num_staging}__{source_name}__staging'
    metadata_path = f"models/operational_metadata/*"
    staging_path = f"models/staging/exact/*" 

    globals()[dag_id] = create_staging_dag(source_name=source_name, data_source_name=data_source_name, default_args=default_args_config_dags, prefix_source=prefix_source, dag_id=dag_id)