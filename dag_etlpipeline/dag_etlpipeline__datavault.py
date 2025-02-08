from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from dags.utils.etl_manager import AirflowETLManager

from datetime import datetime, timedelta
from dags.utils.variables import ETL_DATE, vault_threads, k8s_namespace, dbt_image, SOURCES_DICT1, default_args_config_dags


# Constants
PROFILES_DIR = "/dbt"
PROJECT_DIR = "/dbt"


DATAVAULT_PATHS = {
    "Customer_Relationship": "models/raw_vault/01_Customer_Relationship",
    "Human_Resources": "models/raw_vault/02_Human_Resources",
    "Finance": "models/raw_vault/03_Finance",
    "Items": "models/raw_vault/04_Items",
    "Order": "models/raw_vault/05_Order",
    "Promotion": "models/raw_vault/06_Promotion",
    "Logistics": "models/raw_vault/07_Logistics",
    "Marketing": "models/raw_vault/08_Marketing",
    "Fixed_Asset": "models/raw_vault/09_Fixed_Asset",
    "Manufacture": "models/raw_vault/10_Manufacture"
}

def create_datavault_dag(source_name: str, data_source_name:str, prefix: str, dag_id: str) -> DAG:
    """Function to create a DAG datavault for each source"""
    etl_manager = AirflowETLManager()

    dag_datavault = DAG(
        dag_id,
        default_args=default_args_config_dags,
        description=f'A DAG to run dbt models for {source_name} in the datavault',
        start_date=datetime(2024, 1, 1),
        schedule_interval=None,
        tags=['etl_pipeline', source_name, 'datavault'],
        catchup=False,
        dagrun_timeout=timedelta(minutes=90)
    )

    # Get run_id
    start = PythonOperator(
        task_id='start',
        python_callable=etl_manager.fetch_run_id,
        op_args=[f'{source_name}', f'{dag_id}'],
        provide_context=True,
        retries=5,
        retry_delay=timedelta(seconds=10),
        dag=dag_datavault
    )

    def create_kubernetes_pod_operator(group, path_suffix):
        """Function to create a KubernetesPodOperator for each group"""
        # Check name of sources to write bash cmd
        if source_name.startswith("exact"):
            if source_name == "exact500":
                bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "exact500", "source_name": {source_name}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --exclude tag:appdatashgextappdata tag:multi_sources -s path:{path_suffix}/*"""
            elif source_name == "exact600":
                bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "exact600", "source_name": {source_name}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --exclude tag:appdatashgextappdata tag:multi_sources -s path:{path_suffix}/*"""
            elif source_name == "exact101":
                bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "exact101", "source_name": {source_name}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --exclude tag:appdatashgextappdata tag:multi_sources -s path:{path_suffix}/*"""
            else:
                bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "exact", "source_name_exact": {source_name}, "data_source_name": {data_source_name}, "source_name": {source_name}, "etl_date": "{ETL_DATE}"}}' --exclude tag:appdatashgextappdata tag:multi_sources -s path:{path_suffix}/*"""
        elif source_name.startswith("appdata"):
            if source_name == "appdatashgextappdata":
                bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_appdata": "appdatashgextappdata", "source_name": {source_name}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --exclude tag:exact101 tag:multi_sources -s path:{path_suffix}/*"""
            else:
                bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_appdata": "appdatashgextappdata", "source_name": {source_name}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --exclude tag:exact101 tag:multi_sources -s path:{path_suffix}/appdata/*"""
        elif source_name.startswith("nhamay"):
            if source_name == "nhamay210" or source_name == "nhamay220" or source_name == "nhamay668": 
                bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "nhamay2", "source_name": {source_name}, "source_name_nhamay2": {source_name}, "prefix_sources_nhamay2": {prefix}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --exclude tag:appdatashgextappdata tag:multi_sources -s path:{path_suffix}/*"""
            else:
                bash_command_vault = f"""dbt run --profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR} --vars '{{"source_name_vault": "nhamay", "source_name": {source_name}, "source_name_nhamay": {source_name}, "prefix_sources": {prefix}, "data_source_name": {data_source_name}, "etl_date": "{ETL_DATE}"}}' --exclude tag:appdatashgextappdata tag:multi_sources -s path:{path_suffix}/*"""

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
                'source_name': source_name  
            },
            dag=dag_datavault,
            retries=5,
            retry_delay=timedelta(seconds=60),
        )

    # Create branching operators and tasks
    task_branches = {}
    for group in DATAVAULT_PATHS.keys():
        task_id = group  # Ensure task IDs are in lowercase
        
        check_condition_branch = BranchPythonOperator(
            task_id=f'check_{task_id}',
            python_callable=etl_manager.check_conditions,
            op_kwargs={'task_id': task_id,'source_name': source_name},
            provide_context=True,
            dag=dag_datavault
        )

        # Create raw vault task
        rawvault = create_kubernetes_pod_operator(group, f"{DATAVAULT_PATHS[group]}/*")

        # Define branching logic
        task_branches[task_id] = (check_condition_branch, rawvault)

    # Define end task
    end = PythonOperator(
        task_id='end',
        python_callable=etl_manager.task_end,
        op_args=[f'{dag_id}', f'{source_name}'],
        provide_context=True,
        trigger_rule='all_done',
        dag=dag_datavault,
    )

    # Define DAG structure
    start >> [check for check, _ in task_branches.values()]
    
    for task_id, (check_condition_branch, rawvault) in task_branches.items():
        check_condition_branch >> [rawvault, end]  # Branch to rawvault or businessvault
        rawvault >> end

    return dag_datavault

# Create a DAG datavault for each source
for source_name, descriptions in SOURCES_DICT1.items():
    # Get prefix 
    prefix = descriptions["prefix"]
    data_source_name = descriptions["data_source"]
    num_vault = descriptions["num_vault"]

    dag_id = f'{num_vault}__{source_name}__datavault'

    globals()[dag_id] = create_datavault_dag(source_name, data_source_name, prefix, dag_id)
