from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from dags.utils.etl_manager import AirflowETLManager

from datetime import datetime, timedelta
from dags.utils.variables import ETL_DATE, vault_threads, k8s_namespace, dbt_image, default_args_config_dags


# Constants
PROFILES_DIR = "/dbt"
PROJECT_DIR = "/dbt"

source_dict = {
    # "appdatashgbooking": {"data_source": "appdata_shg_booking", "prefix": ""},
    # "appdatashgextappdata": {"data_source": "appdata_shg_extappdata", "prefix": ""},
    "exact101": {"data_source": "exact_101", "prefix": ""},
    "exact102": {"data_source": "exact_102", "prefix": ""},
    "exact200": {"data_source": "exact_200", "prefix": ""},
    "exact230": {"data_source": "exact_230", "prefix": ""},
    "exact500": {"data_source": "exact_500", "prefix": ""},
    "exact600": {"data_source": "exact_600", "prefix": ""},
    "exact888": {"data_source": "exact_888", "prefix": ""},
    "nhamay210": {"data_source": "nhamay_210", "prefix": "src_210_"},
    "nhamay220": {"data_source": "nhamay_220", "prefix": "src_220_"},
    "nhamay402": {"data_source": "nhamay_402", "prefix": "src_402_"},
    "nhamay405": {"data_source": "nhamay_405", "prefix": "src_405_"},
    "nhamay411": {"data_source": "nhamay_411", "prefix": "src_411_"},
    "nhamay420": {"data_source": "nhamay_420", "prefix": "src_420_"},
    "nhamay466": {"data_source": "nhamay_466", "prefix": "src_466_"},
    "nhamay468": {"data_source": "nhamay_468", "prefix": "src_468_"},
    "nhamay488": {"data_source": "nhamay_488", "prefix": "src_488_"},
    "nhamay501": {"data_source": "nhamay_501", "prefix": "src_501_"},
    "nhamay668": {"data_source": "nhamay_668", "prefix": "src_668_"}
}

"""Function to create a DAG datavault for each source"""
etl_manager = AirflowETLManager()
dag_id = f'21_00__multi_source__datavault'
source_name_default = 'multi_source__datavault'

dag_multi_source_datavault = DAG(
    dag_id,
    default_args=default_args_config_dags,
    description=f'A DAG to run dbt models for model have multi sources in the datavault',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    tags=['etl_pipeline', 'multi_source', 'datavault'],
    catchup=False,
    dagrun_timeout=timedelta(minutes=90)
)

# Get run_id
start = PythonOperator(
    task_id='start',
    python_callable=etl_manager.fetch_run_id,
    op_args=[f'{source_name_default}', f'{dag_id}'],
    provide_context=True,
    retries=5,
    retry_delay=timedelta(seconds=10),
    dag=dag_multi_source_datavault
)

def create_kubernetes_pod_operator(source_name, data_source_name, prefix):
    """Function to create a KubernetesPodOperator for each group"""
    # Check name of sources to write bash cmd
    if source_name.startswith("exact"):
        if source_name == "exact500":
            bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "exact500", "source_name": {source_name}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --select tag:multi_sources"""
        elif source_name == "exact600":
            bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "exact600", "source_name": {source_name}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --select tag:multi_sources"""
        elif source_name == "exact101":
            bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "exact101", "source_name": {source_name}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --select tag:multi_sources"""
        else:
            bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "exact", "source_name_exact": {source_name}, "source_name": {source_name}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --select tag:multi_sources"""
    elif source_name.startswith("nhamay"):
        if source_name == "nhamay210" or source_name == "nhamay220" or source_name == "nhamay668": 
            bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "nhamay2", "source_name": {source_name}, "source_name_nhamay2": {source_name}, "prefix_sources_nhamay2": {prefix}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --select tag:multi_sources"""
        else:
            bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "nhamay", "source_name": {source_name}, "source_name_nhamay": {source_name}, "prefix_sources": {prefix}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --select tag:multi_sources"""

    return KubernetesPodOperator(
        kubernetes_conn_id='k8s_cluster',
        task_id=f'{group}',
        name=f'{group.lower()}',
        namespace=k8s_namespace,
        image=dbt_image,
        cmds=["/bin/bash", "-c", bash_command_vault],
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=False,
        startup_timeout_seconds=7200,
        env_vars={
            'DBT_THREADS': str(vault_threads),
            '_RUN_ID': "{{ task_instance.xcom_pull(task_ids='start', key='_run_id__' ~ params.source_name) }}",
            '_USER_TRINO': str(etl_manager.user_dbt),
            '_PASSWORD_TRINO': str(etl_manager.password_dbt),
            '_DATABASE_TRINO': str(etl_manager.database_dbt),
            '_HOST_TRINO': str(etl_manager.host_dbt),
            '_PORT_TRINO': str(etl_manager.port_dbt)
        },
        params={
            'source_name': source_name_default  
        },
        dag=dag_multi_source_datavault,
        retries=5,
        retry_delay=timedelta(seconds=60),
    )

# Create branching operators and tasks
task_branches = {}
for group, descriptions in source_dict.items():
    task_id = group  # Ensure task IDs are in lowercase
    
    check_condition_branch = BranchPythonOperator(
        task_id=f'check_{task_id}',
        python_callable=etl_manager.check_conditions,
        op_kwargs={'task_id': task_id,'source_name': source_name_default},
        provide_context=True,
        dag=dag_multi_source_datavault
    )

    # Get prefix 
    prefix = descriptions["prefix"]
    data_source_name = descriptions["data_source"]
        
    # Create raw vault task
    rawvault = create_kubernetes_pod_operator(task_id, data_source_name, prefix)

    # Define branching logic
    task_branches[task_id] = (check_condition_branch, rawvault)

# Define end task
end = PythonOperator(
    task_id='end',
    python_callable=etl_manager.task_end,
    op_args=[f'{dag_id}', f'{source_name_default}'],
    provide_context=True,
    trigger_rule='all_done',
    dag=dag_multi_source_datavault,
)

# Define DAG structure
start >> [check for check, _ in task_branches.values()]

for task_id, (check_condition_branch, rawvault) in task_branches.items():
    check_condition_branch >> [rawvault, end]  # Branch to rawvault or businessvault
    rawvault >> end


