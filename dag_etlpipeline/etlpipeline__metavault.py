from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from dags.utils.etl_manager import metaVault
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}
metaVault = metaVault()

with DAG(
    dag_id=f'etlpipeline__meta_Vault',
    default_args=default_args,
    description=f'A DAG to get and update meta vault of etl_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    tags=['meta_vault'],
    catchup=False
) as dag_metavault:

    start = DummyOperator(
        task_id='start',
        dag=dag_metavault
    )

    end = DummyOperator(
        task_id='end',
        dag=dag_metavault
    )

    hub_concept = PythonOperator(
        task_id='hub_concept',
        python_callable=metaVault.get_hub_concept,
        dag=dag_metavault
    )

    hub_hub = PythonOperator(
        task_id='hub_hub',
        python_callable=metaVault.get_hub,
        dag=dag_metavault
    )

    link_concept_entity = PythonOperator(
        task_id='link_concept_entity',
        python_callable=metaVault.get_link_concept_entity,
        dag=dag_metavault
    )

    link_between_entity = PythonOperator(
        task_id='link_between_entitys',
        python_callable=metaVault.get_link_between_entity,
        dag=dag_metavault
    )

    satellite_entity = PythonOperator(
        task_id='satellite_entity',
        python_callable=metaVault.get_satellite_entity,
        dag=dag_metavault
    )

    satellite_satellite_detail_entity = PythonOperator(
        task_id='satellite_satellite_detail_entity',
        python_callable=metaVault.get_satellite_satellite_detail_entity,
        dag=dag_metavault
    )

    start >> [hub_concept, hub_hub, link_concept_entity, link_between_entity, satellite_entity, satellite_satellite_detail_entity] >> end