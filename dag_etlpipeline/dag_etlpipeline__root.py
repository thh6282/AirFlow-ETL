from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from dags.utils.etl_manager import AirflowETLManager
from airflow.sensors.time_sensor import TimeSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator

import pytz
from dags.utils.variables import ETL_DATE, SOURCES_DICT1, etlpipeline_schedule, default_args_config_dags

from dags.utils.test_num_of_records import check_num_of_records
from dags.utils.send_email import generate_content_email_etl_completed_for_all_source, get_recipient_email_list_etl_completed, update_email_notification_etl_completed_for_all_source, generate_content_email_etl_pipeline_error, check_etl_pipeline_error, update_email_notification_etl_error

# Define the ETL Manager object
etl_manager = AirflowETLManager()
bangkok_tz = pytz.timezone('Asia/Bangkok')
# Get time crrent follow timezone 'Asia/Bangkok'
current_time_bangkok = datetime.now(bangkok_tz)

with DAG(
    '00__etlpipeline__root', 
    default_args=default_args_config_dags, 
    schedule_interval=f'{etlpipeline_schedule}',
    tags=['etl_pipeline', 'admin'],
    catchup=False
) as dag:

    # Task to start by updating ETL dates
    start = PythonOperator(
        task_id='start',
        python_callable=etl_manager.check_and_update_status_etl_dates,
        retries=5,
        retry_delay=timedelta(seconds=10),
        trigger_rule='none_skipped',
        provide_context=True,
        dag=dag,
    )

    # Task to update pipeline's status after all master DAGs
    end = PythonOperator(
        task_id='end',
        python_callable=etl_manager.task_end,
        op_args=[f'00__etlpipeline__root'],
        retries=5,
        retry_delay=timedelta(seconds=10),
        trigger_rule='all_done',
        dag=dag,
    )

    test = PythonOperator(
        task_id='check_records',
        python_callable=check_num_of_records,
        trigger_rule='all_done',
        dag=dag,
    )

    multi_source__datavault = TriggerDagRunOperator(
        task_id=f'multi_source__datavault',
        trigger_dag_id=f'21_00__multi_source__datavault',
        wait_for_completion=True,
        allowed_states=['success'],
        trigger_rule='none_skipped'
    )
    
    # List to collect all TaskGroup objects for later linking
    all_groups = []

    for source_name, values in SOURCES_DICT1.items():
        wait_time = values.get('wait_time')
        num_staging = values.get("num_staging")
        num_vault = values.get("num_vault")

        # Create a TaskGroup for each source
        with TaskGroup(group_id=f'etlpipeline__{source_name}') as group:
            # Create sensor for each source
            time_sensor = TimeSensor(
                task_id=f'wait_time__{source_name}',
                target_time=(current_time_bangkok + timedelta(minutes=wait_time)).time(),
                dag=dag
            )

            # insert data and get run_id to table info_etlpipeline_by_sources
            start__source_name = PythonOperator(
                task_id=f'start__{source_name}',
                python_callable=etl_manager.insert_into_info_by_sources,
                op_args=[f'{source_name}', f'etlpipeline__{source_name}__group'],
                retries=5,
                retry_delay=timedelta(seconds=10),
                trigger_rule='none_skipped',
                dag=dag,
            )           

            dag_staging = TriggerDagRunOperator(
                task_id=f'{num_staging}__{source_name}__staging',
                trigger_dag_id=f'{num_staging}__{source_name}__staging',
                wait_for_completion=True,
                allowed_states=['success'],
                conf={'triggered_by': 'group'},
                trigger_rule='none_skipped'
            )
                        
            dag_datavault = TriggerDagRunOperator(
                task_id=f'{num_vault}__{source_name}__datavault',
                trigger_dag_id=f'{num_vault}__{source_name}__datavault',
                wait_for_completion=True,
                allowed_states=['success'],
                conf={'triggered_by': 'group'},
                trigger_rule='none_skipped'
            )

            # Task to update the pipeline's status after datamart DAG completes
            end__source_name = PythonOperator(
                task_id=f'end__{source_name}',
                python_callable=etl_manager.task_end,
                op_args=[f'etlpipeline__{source_name}__group', f'{source_name}'],
                retries=1,
                retry_delay=timedelta(seconds=10),
                trigger_rule='none_skipped',
                dag=dag,
            )

            # Set dependencies within the group
            time_sensor >> start__source_name >> dag_staging >> dag_datavault >> end__source_name
            # Append the group to the list of groups
            all_groups.append(group)

    with TaskGroup(group_id='send_email_etl_completed') as send_email_complete_group:
        # Task to send email notification
        # Task to send email
        send_email_task = EmailOperator(
            task_id=f'send_email_task_status_all_sources',
            to="{{ task_instance.xcom_pull(task_ids='send_email_etl_completed.recipient_email_list') | join(',')}}",
            subject=f'ETL Pipeline Status for all sources on {ETL_DATE}',
            html_content="{{ task_instance.xcom_pull(task_ids='send_email_etl_completed.generate_content_email') }}",
            conn_id='k8s_email',
            trigger_rule=TriggerRule.ALL_DONE
        )

        # Tasks to get email list and content
        get_recipient_email_list_task = PythonOperator(
            task_id='recipient_email_list',
            python_callable=get_recipient_email_list_etl_completed,
            do_xcom_push=True,
            trigger_rule=TriggerRule.ALL_DONE
        )

        generate_content_email_task = PythonOperator(
            task_id='generate_content_email',
            python_callable=generate_content_email_etl_completed_for_all_source,
            do_xcom_push=True,
            trigger_rule=TriggerRule.ALL_DONE
        )

        # Task to update email notification status
        update_email_notification_task = PythonOperator(
            task_id=f'update_email_notification_task_all_sources',
            python_callable=update_email_notification_etl_completed_for_all_source,
            trigger_rule=TriggerRule.ALL_DONE
        )

        [get_recipient_email_list_task, generate_content_email_task] >> send_email_task
        send_email_task >> update_email_notification_task
        # Set dependency from start to each group
    with TaskGroup(group_id='send_email_etl_error') as send_email_error_group:
        # Task to send email notification
        # Task to send email
        send_email_task = EmailOperator(
            task_id=f'send_email_task_status_all_sources',
            to="{{ task_instance.xcom_pull(task_ids='send_email_etl_error.recipient_email_list') | join(',')}}",
            subject=f'[ERROR] ETL Pipeline Error on {ETL_DATE}',
            html_content="{{ task_instance.xcom_pull(task_ids='send_email_etl_error.generate_content_email') }}",
            conn_id='k8s_email',
            trigger_rule=TriggerRule.NONE_SKIPPED
        )

        # Tasks to get email list and content
        get_recipient_email_list_task = PythonOperator(
            task_id='recipient_email_list',
            python_callable=get_recipient_email_list_etl_completed,
            do_xcom_push=True,
            trigger_rule=TriggerRule.NONE_SKIPPED
        )

        generate_content_email_task = PythonOperator(
            task_id='generate_content_email',
            python_callable=generate_content_email_etl_pipeline_error,
            do_xcom_push=True,
            trigger_rule=TriggerRule.NONE_SKIPPED
        )

        # Task to update email notification status
        update_email_notification_task = PythonOperator(
            task_id=f'update_email_notification_task_all_sources',
            python_callable=update_email_notification_etl_error,
            trigger_rule=TriggerRule.NONE_SKIPPED
        )

        get_recipient_email_list_task >> generate_content_email_task >> send_email_task
        send_email_task >> update_email_notification_task

    check_etl_error = BranchPythonOperator(
        task_id='check_etl_error',
        python_callable=check_etl_pipeline_error,
        provide_context=True,
        
    )


start >> all_groups >> multi_source__datavault >> send_email_complete_group >> test  >>  end
start >> all_groups >> multi_source__datavault >> check_etl_error >> send_email_error_group  >> test >>  end




