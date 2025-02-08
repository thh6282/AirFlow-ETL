from airflow.models import DagRun, TaskInstance, Variable
from airflow import settings
from sqlalchemy import desc
from trino.dbapi import connect
import uuid as uuid_module
import os
import yaml
import logging
from dags.utils.variables import ETL_DATE, trino_host, trino_port, trino_catalog, trino_schema, trino_user, trino_password

RED_BOLD = '\033[1;31m'
ORANGE = '\033[38;5;214m'  # Màu cam
GREEN = '\033[32m'           # Màu xanh
RESET = '\033[0m'           # Đặt lại màu về mặc định

class AirflowETLManager:
    def __init__(self):
        self.host_connect = trino_host
        self.port_connect = trino_port
        self.user = "admin"

        self.catalog = trino_catalog
        self.schema = trino_schema
        self.etl_dates_table = "etl_dates"
        self.info_table = "info_etlpipeline_by_sources"
        self.status_etlpipeline_task_airflow_table = "status_etlpipeline_task_airflow"

        # Config connection trino for dbt
        self.user_dbt = trino_user
        self.password_dbt = trino_password
        self.database_dbt = trino_catalog
        self.host_dbt = trino_host
        self.port_dbt = trino_port

    def _get_connection(self):
        """Create and return a Trino connection."""
        return connect(
            host=self.host_connect,
            port=self.port_connect,
            user=self.user
        )

    def _get_etl_date(self) -> str:
        """Retrieve the ETL date from the Trino database."""
        conn = self._get_connection()
        cur = conn.cursor()

        query_get_etl_date = f"""
        SELECT etl_date FROM {self.catalog}.{self.schema}.{self.etl_dates_table}
        WHERE holiday_flag = 0 and status IS NULL
        ORDER BY etl_date 
        LIMIT 1
        """

        query_max_date = f"""
        SELECT etl_date FROM {self.catalog}.{self.schema}.{self.etl_dates_table}
        WHERE holiday_flag = 0
        ORDER BY etl_date DESC
        LIMIT 1
        """

        try:
            cur.execute(query_get_etl_date)
            etl_date = cur.fetchall()[0][0]
        except:
            cur.execute(query_max_date)
            etl_date = cur.fetchall()[0][0]
        cur.close()

        return etl_date

    def _get_run_id(self, source_name) -> str:
        """Retrieve the run ID from the Trino database."""
        conn = self._get_connection()
        cur = conn.cursor()

        query_get_run_id = f"""
            SELECT run_id 
            FROM {self.catalog}.{self.schema}.{self.info_table}
            WHERE etl_date = '{ETL_DATE}' 
            AND source_name = '{source_name}'
            AND created_at = (
                SELECT MAX(created_at) 
                FROM {self.catalog}.{self.schema}.{self.info_table}
                WHERE etl_date = '{ETL_DATE}' 
                    AND source_name = '{source_name}'
            )
        """

        try:
            cur.execute(query_get_run_id)
            run_id_ = cur.fetchall()
        except:
            run_id_ = []
            
        if run_id_ != []:
            run_id = run_id_[0][0]
        else:
            run_id = "abc"
        cur.close()

        return run_id

    def save_status_task(self, dag_id, source_name, triggered_by) -> None:
        """Save the status of tasks to the Trino database."""
        conn = self._get_connection()
        cur = conn.cursor()

        etl_date = ETL_DATE

        # ti = kwargs['ti']
        # run_id = ti.xcom_pull(task_ids='start', key=f'_run_id__{source_name}')
        run_id = self._get_run_id(source_name)
        print(f"Saving status for DAG {dag_id} with run_id {run_id}")

        # Create a session to interact with the Airflow metadata database
        session = settings.Session()

        # Retrieve the latest DAG run for the specified DAG ID
        latest_dag_run = session.query(DagRun).filter(DagRun.dag_id == dag_id).order_by(desc(DagRun.execution_date)).first()

        if latest_dag_run:
            print(f"Latest DAG Run ID: {latest_dag_run.run_id}, State: {latest_dag_run.state}, Execution Date: {latest_dag_run.execution_date}")

            # Retrieve all task instances for the latest DAG run
            task_instances = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == latest_dag_run.run_id
            ).all()

            full_info = []

            # Iterate over the task instances and collect their details
            for task_instance in task_instances:
                if task_instance.task_id not in ('start', 'end') and not task_instance.task_id.startswith('check'):

                    task_info = (
                        f"('{run_id}', '{source_name}', '{dag_id}', '{task_instance.task_id}', "
                        f"'{task_instance.state}', TIMESTAMP '{task_instance.start_date}', "
                        f"TIMESTAMP '{task_instance.end_date}', '{etl_date}', CURRENT_TIMESTAMP, 'airflow')"
                    )

                    full_info.append(task_info)

            values_insert = ',\n'.join(full_info)

            # Insert into the table
            insert_table = f"""
            INSERT INTO {self.catalog}.{self.schema}.{self.status_etlpipeline_task_airflow_table} 
            (run_id, source_name, dag_id, task_id, status, start_time, end_time, etl_date, created_at, created_by)
            VALUES {values_insert}
            """

            # transaction
            cur.execute(insert_table)
            conn.commit()

            # confirm update successful
            logging.info(f"{GREEN}Updated information into {ORANGE}status_etlpipeline_task_airflow{RESET}:\n{GREEN}{values_insert}{RESET}")
        else:
            print(f"No DAG runs found for DAG ID: {dag_id}")

        # Close the session
        session.close()

        # When save status done will update status of table info from running to success or faild
        # triggered_by = kwargs['dag_run'].conf.get('triggered_by')
        if triggered_by == 'master_dag' or triggered_by == 'group':
            cur.close()
            conn.close()
        else:
            try:
                # Execute a query to get the status of all tasks executed in the current thread
                query_check_status_with_dag = f"""
                SELECT 
                    status
                FROM 
                    {self.catalog}.{self.schema}.{self.status_etlpipeline_task_airflow_table}
                WHERE 
                    run_id = '{run_id}' 
                """

                cur.execute(query_check_status_with_dag)
                statuss = cur.fetchall()

                print(statuss)

                # Check if all tasks have succeeded
                all_success = all(status[0] in ('success', 'skipped') for status in statuss)

                print(f"status is: {all_success}")

                if all_success:
                    # Update status to 'success' in info_table
                    query_update_success = f"""
                    UPDATE {self.catalog}.{self.schema}.{self.info_table}
                    SET status = 'success', updated_at = CURRENT_TIMESTAMP
                    WHERE run_id = '{run_id}'
                    """
                    cur.execute(query_update_success)
                    print("ETL status updated to 'success'.")
                else:
                    # Update status to 'failed' in info_table
                    query_update_failed = f"""
                    UPDATE {self.catalog}.{self.schema}.{self.info_table}
                    SET status = 'failed', updated_at = CURRENT_TIMESTAMP
                    WHERE run_id = '{run_id}'
                    """
                    cur.execute(query_update_failed)
                    print("ETL status updated to 'failed'.")

                cur.close()
                conn.close()

            except Exception as e:
                print(f"An error occurred while updating the ETL status: {str(e)}")


    def update_status_etl(self) -> None:
        """Update the ETL status in the Trino database based on task execution results."""
        conn = self._get_connection()
        cur = conn.cursor()

        etl_date = ETL_DATE

        # Execute a query to get the status of all tasks executed in the current thread
        query_check_status = f"""
            WITH ranked_sources AS (
                SELECT source_name, status, created_at, updated_at,
                    ROW_NUMBER() OVER (PARTITION BY source_name ORDER BY created_at DESC) AS rn
                FROM {self.catalog}.{self.schema}.{self.info_table}
                where etl_date = '{etl_date}' AND created_by IN ('master', 'group')
            )

            select status from ranked_sources
            where rn = 1
        """

        cur.execute(query_check_status)
        statuss = cur.fetchall()

        print(statuss)

        # Check if all tasks have succeeded
        all_success = all(status[0] in ('success', 'skipped') for status in statuss)

        logging.info(f"\033[1;91mstatus is: {all_success}\033[0m")

        if all_success:
            # Update status to 'success' in etl_dates_table
            query_update_success = f"""
            UPDATE {self.catalog}.{self.schema}.{self.etl_dates_table}
            SET status = 'success', sysupdated_at = CURRENT_TIMESTAMP
            WHERE etl_date = '{etl_date}'
            """
            cur.execute(query_update_success)
            print("ETL status updated to 'success'.")
        else:
            # Update status to 'failed' in etl_dates_table
            query_update_failed = f"""
            UPDATE {self.catalog}.{self.schema}.{self.etl_dates_table}
            SET status = 'failed', sysupdated_at = CURRENT_TIMESTAMP
            WHERE etl_date = '{etl_date}'
            """
            cur.execute(query_update_failed)
            print("ETL status updated to 'failed'.")

        cur.close()

    #check dag is run as master trigger or self trigger
    def fetch_run_id(self, source_name, dag_id, **kwargs) -> str:
        triggered_by = kwargs['dag_run'].conf.get('triggered_by')
        if triggered_by == 'master_dag' or triggered_by == 'group':
            run_id = self._get_run_id(source_name) # it is run_id get from table etl_dates

            logging.info(f"\033[1;32mStart running {dag_id} of {source_name} with run_id is: {run_id} \033[0m")
        else:
            run_id = str(uuid_module.uuid4()) # it is new run_id
            conn = self._get_connection()
            cur = conn.cursor()

            # Split dag_id to get name of dag trigger insert into table info
            dag_type = dag_id.split('__')[-1]

            query_update_run_id = f"""
            INSERT INTO {self.catalog}.{self.schema}.{self.info_table} (run_id, source_name, etl_date, status, created_at, created_by)
            values ('{run_id}', '{source_name}', '{ETL_DATE}', 'running', CURRENT_TIMESTAMP, '{dag_type}')
            """

            # transaction
            cur.execute(query_update_run_id)
            conn.commit()

            logging.info(f"\033[1;32mStart running {dag_id} of {source_name} with run_id is: {run_id}\033[0m")
            cur.close()
            conn.close()
            
        # Lưu run_id vào XComs
        kwargs['ti'].xcom_push(key=f'_run_id__{source_name}', value=run_id)

        return run_id
    
    # Check and update status, and current_date_flag of table etl_dates, load etl_date to the variable in airflow
    def check_and_update_status_etl_dates(self, **kwargs) -> None:
        conn = self._get_connection()
        cur = conn.cursor()

        if kwargs['dag_run'] == None or (kwargs['dag_run'] != None and kwargs['dag_run'].external_trigger):
            print("This is an external run.  Mark it as such")
        else:
            print("This is a scheduled run")
            etl_date = self._get_etl_date()
            # write etl_date to the variable in airflow
            Variable.set("datavault__etlpipeline__etldate", etl_date)

        # uuid = str(uuid_module.uuid4())

        # Execute to update current_date_flag
        query_update = f"""
        UPDATE {self.catalog}.{self.schema}.{self.etl_dates_table}
        SET current_date_flag = 0
        """

        # Get date from variable etl_dates
        ETL_DATE = Variable.get("datavault__etlpipeline__etldate")

        logging.info(f"\033[1;32mStart running ETL_PIPELINE with etl_date: {ETL_DATE}\033[0m")

        # Execute the query to update status of table etl_dates
        query = f"""
        UPDATE {self.catalog}.{self.schema}.{self.etl_dates_table}
        SET status = 'running', updated_at = CURRENT_TIMESTAMP, current_date_flag = 1
        WHERE etl_date = '{ETL_DATE}'
        """

        cur.execute(query_update)
        cur.execute(query)

        # delete data in table schemadrift_staging_temp, schemadrift_source_temp, schemadrift_change_temp daily before run pipeline
        delete_stg_table_tmp_query = f"""
        Delete from {self.catalog}.{self.schema}.schemadrift_staging_temp
        """

        delete_src_table_tmp_query = f"""
        Delete from {self.catalog}.{self.schema}.schemadrift_source_temp
        """

        delete_change_table_tmp_query = f"""
        Delete from {self.catalog}.{self.schema}.schemadrift_change_temp
        """
        cur.execute(delete_change_table_tmp_query)
        cur.execute(delete_src_table_tmp_query)
        cur.execute(delete_stg_table_tmp_query)

        cur.close()


    # Create def to insert data table info with sources and etl_date
    def insert_into_info_by_sources(self, source_name, dag_id) -> None:
        conn = self._get_connection()
        cur = conn.cursor()

        # Get date from variable etl_dates
        # ETL_DATE = Variable.get("datavault__etlpipeline__etldate")

        uuid = str(uuid_module.uuid4())

        # Split dag_id to get name of dag trigger insert into table info
        dag_type = dag_id.split('__')[-1]

        # Execute the query to insert data to the table info
        query_insert = f"""
        insert into {self.catalog}.{self.schema}.{self.info_table} (run_id, source_name, etl_date, status, created_at, created_by)
        values ('{uuid}', '{source_name}', '{ETL_DATE}', 'running', CURRENT_TIMESTAMP, '{dag_type}')
        """

        # transaction
        cur.execute(query_insert)
        conn.commit()

        cur.close()
        conn.close()

    # create def to update data with source and etl_date in table info
    def update_into_info_by_sources(self, source_name) -> None:
        conn = self._get_connection()
        cur = conn.cursor()

        # Get run_id
        run_id = self._get_run_id(source_name)

        # Execute a query to get the status of all tasks executed in the current thread
        query_check_status = f"""
        SELECT 
            status
        FROM 
            {self.catalog}.{self.schema}.{self.status_etlpipeline_task_airflow_table}
        WHERE 
            run_id = '{run_id}' 
        """

        cur.execute(query_check_status)
        statuss = cur.fetchall()

        print(statuss)

        # Check if all tasks have succeeded
        all_success = all(status[0] in ('success', 'skipped') for status in statuss)

        if all_success:
            # Update status to 'success' in etl_dates_table
            query_update_success = f"""
            UPDATE {self.catalog}.{self.schema}.{self.info_table}
            SET status = 'success', updated_at = CURRENT_TIMESTAMP
            WHERE run_id = '{run_id}'
            """
            cur.execute(query_update_success)
            conn.commit()
            logging.info(f"{GREEN}ETL of {source_name} at {ETL_DATE} status updated to: Success{RESET}")
        else:
            # Update status to 'failed' in etl_dates_table
            query_update_failed = f"""
            UPDATE {self.catalog}.{self.schema}.{self.info_table}
            SET status = 'failed', updated_at = CURRENT_TIMESTAMP
            WHERE run_id = '{run_id}'
            """
            cur.execute(query_update_failed)
            conn.commit()
            logging.info(f"{RED_BOLD}ETL of {source_name} at {ETL_DATE} status updated to: Success{RESET}")

        cur.close()
        conn.close()

    # check condition to ignore task in airflow with task is successed
    def check_conditions(self, task_id, source_name) -> str:
        """Check if a task should be skipped based on its status"""
        etl_date = ETL_DATE
        conn = self._get_connection()
        cur = conn.cursor()

        query_check = f"""
        SELECT 
            status
        FROM 
            {self.catalog}.{self.schema}.{self.status_etlpipeline_task_airflow_table}
        WHERE etl_date = '{etl_date}' 
        AND (TRY(split(task_id, '.')[2]) = '{task_id}' OR task_id = '{task_id}') 
        AND source_name = '{source_name}'
        """

        try:
            cur.execute(query_check)
            status_check_task = cur.fetchall()
        except Exception as e:
            status_check_task = []
            # Log exception if needed
            print(f"Error querying status: {e}")

        # Check if any task has succeeded
        check_success = any(status[0] == 'success' for status in status_check_task)

        # Determine which path to take based on the task's status

        
        if check_success:
            return f"end"
        else:
            return task_id


    def check_state_of_dag(self, dag_id, source_name):
        conn = self._get_connection()
        cur = conn.cursor()

        # Get run_id
        run_id = self._get_run_id(source_name)

        query_check_for_staging_datavault = f"""
        SELECT 
            status
        FROM 
            {self.catalog}.{self.schema}.{self.status_etlpipeline_task_airflow_table}
        WHERE run_id = '{run_id}' and dag_id = '{dag_id}'
        """

        query_check_for_master = f"""
        SELECT 
            status
        FROM 
            {self.catalog}.{self.schema}.{self.info_table}
        WHERE run_id = '{run_id}' 
        """

        query_check_for_admin = f"""
        SELECT 
            status
        FROM 
            {self.catalog}.{self.schema}.{self.etl_dates_table}
        WHERE etl_date = '{ETL_DATE}' 
        """

        if 'master' in dag_id or 'group' in dag_id:
            try:
                cur.execute(query_check_for_master)
                check_dags = cur.fetchall()
            except Exception as e:
                check_dags = []
                # Log exception if needed
                print(f"Error querying status: {e}")

            condition = all(status[0] in ('success') for status in check_dags)

            if condition:
                print(f'All tasks of Dag {dag_id} are succesed: {check_dags}')
            else:
                raise ValueError(f"Condition not met, Dag fails. {check_dags}")
            
        elif 'admin' in dag_id or 'root' in dag_id:
            try:
                cur.execute(query_check_for_admin)
                check_dags = cur.fetchall()
            except Exception as e:
                check_dags = []
                # Log exception if needed
                print(f"Error querying status: {e}")

            condition = all(status[0] in ('success') for status in check_dags)

            if condition:
                print(f'All tasks of Dag {dag_id} are succesed: {check_dags}')
            else:
                raise ValueError(f"Condition not met, Dag fails. {check_dags}")
            
        else: 
            try:
                cur.execute(query_check_for_staging_datavault)
                check_dags = cur.fetchall()
            except Exception as e:
                check_dags = []
                # Log exception if needed
                print(f"Error querying status: {e}")

            condition = all(status[0] in ('success', 'skipped') for status in check_dags)

            if condition:
                print(f'All tasks of Dag {dag_id} are succesed: {check_dags}')
            else:
                raise ValueError(f"Condition not met, Dag fails. {check_dags}")
            
    def task_end(self, dag_id, source_name = 'admin', **kwargs) -> None:
        # ------------------------------------------------------ #
        #     This is code to check and update the state         #
        #             of the end task in admin                   #
        # ------------------------------------------------------ #
        if 'admin' in dag_id or 'root' in dag_id:
            # update status of table etl_dates
            self.update_status_etl()

            # check status for task end
            self.check_state_of_dag(dag_id=dag_id, source_name=source_name)

        # ------------------------------------------------------ #
        #     This is code to check and update the state         #
        #             of the end task in master                  #
        # ------------------------------------------------------ #            
        elif 'master' in dag_id or 'group' in dag_id:
            # update status of table info
            self.update_into_info_by_sources(source_name=source_name)

            # check status for task end
            self.check_state_of_dag(dag_id=dag_id, source_name=source_name)

        # ------------------------------------------------------ #
        #     This is code to check and update the state         #
        #        of the end task in staging and vault            #
        # ------------------------------------------------------ #            
        else:
            triggered_by = kwargs['dag_run'].conf.get('triggered_by')
            # save state of all task into table status
            self.save_status_task(dag_id=dag_id, source_name=source_name, triggered_by=triggered_by)

            # check status for task end
            self.check_state_of_dag(dag_id=dag_id, source_name=source_name)     

class metaVault():
    def __init__(self):
        self.host_connect = trino_host
        self.port_connect = trino_port
        self.user = "admin"

        self.path = '/opt/airflow/dags/repo/models/raw_vault'

    def _get_connection(self):
        """Create and return a Trino connection."""
        return connect(
            host=self.host_connect,
            port=self.port_connect,
            user=self.user
        )
    
    # Function to get concept and update into table hub_concept
    def get_hub_concept(self):
        conn = self._get_connection()
        cur = conn.cursor()
        insert_values = []
        insert_values_total = []

        for folder in os.listdir(self.path):
            folder_path = os.path.join(self.path, folder)
            if os.path.isdir(folder_path):
                folder_name = folder_path.split('/')[-1][3:]
                values = f"""{folder_name}"""
                insert_values.append(values)

        insert_values_total.append(insert_values)

        query_insert = f"""
        MERGE INTO dp_warehouse.meta_vault.hub_concept t 
        using (
            SELECT 
                concept_name
            FROM 
                UNNEST(
                        ARRAY{insert_values_total[0]} 
                ) AS t(concept_name)
        ) s
            on (t.concept_name = s.concept_name)
            when matched 
                then update set concept_name = s.concept_name
            when not matched 
                then insert (concept_name)
                    values (s.concept_name)
        """

        cur.execute(query_insert)
        conn.commit()

    # Function to get data for hub table of meta vault
    def get_hub(self):
        conn = self._get_connection()
        cur = conn.cursor()

        info = {'hub':[[],[],[]], 'link':[[],[],[]], 'satellite':[[],[],[]]}

        for folder in os.listdir(self.path):
            folder_path = os.path.join(self.path, folder)
            
            if os.path.isdir(folder_path):  # check see is folder or not
                # find all file .yml in folder
                for file in os.listdir(folder_path):
                    if file.endswith('.yml'):
                        file_path = os.path.join(folder_path, file)
                        
                        # read content of file .yml
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = yaml.safe_load(f)
                            
                            for enti in content['models']:
                                name = str(enti['name']).replace(' ', '_').lower()

                                if name.startswith('hub'):
                                    # name
                                    info['hub'][0].append(f'{name}')
                                    #description
                                    info['hub'][1].append(f"{enti['description']}")
                                    #notes
                                    info['hub'][2].append("NULL")
                                elif name.startswith('link'):
                                    # name
                                    info['link'][0].append(f'{name}')
                                    #description
                                    info['link'][1].append(f"{enti['description']}")
                                    #notes
                                    info['link'][2].append("NULL")
                                elif name.startswith('satellite'):
                                    # name
                                    info['satellite'][0].append(f'{name}')
                                    #description
                                    info['satellite'][1].append(f"{enti['description']}")
                                    #notes
                                    info['satellite'][2].append("NULL")

        print(info)
        for entity, values in info.items():
            # get values insert
            # query to insert into meta vault
            query_insert = f"""
            MERGE INTO dp_warehouse.meta_vault.hub_{entity} t 
            using (
                SELECT 
                    {entity}_name,
                    {entity}_description,
                    notes
                FROM 
                    UNNEST(
                        zip (
                            ARRAY{values[0]},
                            ARRAY{values[1]},
                            ARRAY{values[2]}
                            )
                    ) AS t({entity}_name, {entity}_description, notes)
            ) s
                on (t.{entity}_name = s.{entity}_name)
                when matched 
                    then update set {entity}_name = s.{entity}_name, {entity}_description = s.{entity}_description, notes = NULL
                when not matched 
                    then insert ({entity}_name, {entity}_description, notes)
                        values (s.{entity}_name, s.{entity}_description, s.notes)
            """

            cur.execute(query_insert)
            conn.commit()

    # function to get and update for table link_concept_entity of meta vault
    def get_link_concept_entity(self):
        conn = self._get_connection()
        cur = conn.cursor()
        
        # list model have in code of mode
        list_models = []
        # loop through directories in given path
        for foldername in os.listdir(self.path):
            folder_path = os.path.join(self.path, foldername)
            if os.path.isdir(folder_path):
                # get list directories or file in current directories
                for entity in os.listdir(folder_path):
                    folder_path_entity = os.path.join(folder_path, entity)
                    if os.path.isdir(folder_path_entity):
                        for model in os.listdir(folder_path_entity):
                            # get model of current folder
                            list_models.append(model.split('.')[0])
        print(list_models)

        # get daraa from file .yaml
        info = {}
        for folder in os.listdir(self.path):
            folder_path = os.path.join(self.path, folder) 
            if os.path.isdir(folder_path):
                concept = folder.split('.')[0][3:]
                info[f"{concept}"] = []
                print(concept) 
                # file all file .yml in folder
                for file in os.listdir(folder_path):
                    if file.endswith('.yml'):
                        file_path = os.path.join(folder_path, file)
                        
                        # read content of file .yml
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = yaml.safe_load(f)
                            for enti in content['models']:
                                name = str(enti['name']).replace(' ', '_').lower()
                                info[f"{concept}"].append(name)

        # check if existing model are in use
        detal_info = {"concept_name": [], "entity_name": []}

        for concept, entity_names in info.items():
            for entity in entity_names:
                detal_info["concept_name"].append(concept)
                detal_info["entity_name"].append(entity)

        # check if these is new model, if there is, insert it into the link_concept_entity table of meta Vault 
        check_new_model = f"""
            insert into dp_warehouse.meta_vault.link_concept_entity 
            with src as (
                SELECT 
                    concept_name,
                    entity_name
                FROM 
                    UNNEST(
                        zip (
                            ARRAY{detal_info["concept_name"]},
                            ARRAY{detal_info["entity_name"]}
                            )
                    ) AS t(concept_name, entity_name)
            ),
            
            -- Check concept and model don't have in link_concept_entity
            model as (
            SELECT
                s.concept_name,
                s.entity_name
            FROM src s
            LEFT JOIN dp_warehouse.meta_vault.link_concept_entity t
            ON s.concept_name = t.concept_name AND s.entity_name = t.entity_name
            WHERE t.concept_name is NULL and t.entity_name is NULL
            )
            
            select 
                concept_name,
                entity_name,
                1
            FROM model
        """

        cur.execute(check_new_model)
        conn.commit()

        # check activa_status of model
        # check_active_status = f"""
        # MERGE INTO dp_warehouse.meta_vault.link_concept_entity t 
        # using (
        #     SELECT 
        #         concept_name,
        #         entity_name
        #     FROM 
        #         UNNEST(
        #             zip (
        #                 ARRAY{detal_info["concept_name"]},
        #                 ARRAY{detal_info["entity_name"]}
        #                 )
        #         ) AS t(concept_name, entity_name)
        # ) s
        #     on (t.concept_name = s.concept_name and t.entity_name = s.entity_name)
        #     when matched 
        #         then update set active_status = 1
        #     when not matched 
        #         then update set active_status = 0
        # """

        check_active_status_when_not_have_model = f"""
        UPDATE dp_warehouse.meta_vault.link_concept_entity
        SET active_status = 0
        WHERE entity_name in (
            SELECT
                t.entity_name
            FROM dp_warehouse.meta_vault.link_concept_entity t
            LEFT JOIN (
                SELECT 
                    concept_name,
                    entity_name
                FROM 
                    UNNEST(
                        zip (
                            ARRAY{detal_info["concept_name"]},
                            ARRAY{detal_info["entity_name"]}
                            )
                    ) AS t(concept_name, entity_name)
            ) s
            ON s.concept_name = t.concept_name AND s.entity_name = t.entity_name
            WHERE s.concept_name is NULL and s.entity_name is NULL
        )
        """

        check_active_status_when_have_model = f"""
        UPDATE dp_warehouse.meta_vault.link_concept_entity
        SET active_status = 1
        WHERE entity_name in (
            SELECT
                t.entity_name
            FROM dp_warehouse.meta_vault.link_concept_entity t
            JOIN (
                SELECT 
                    concept_name,
                    entity_name
                FROM 
                    UNNEST(
                        zip (
                            ARRAY{detal_info["concept_name"]},
                            ARRAY{detal_info["entity_name"]}
                            )
                    ) AS t(concept_name, entity_name)
            ) s
            ON s.concept_name = t.concept_name AND s.entity_name = t.entity_name
        )
        """

        cur.execute(check_active_status_when_not_have_model)
        cur.execute(check_active_status_when_have_model)
        conn.commit()

    # Create function to check mapping between hub, link, satellite tables of all table link in meta vault
    def get_link_between_entity(self):
        conn = self._get_connection()
        cur = conn.cursor()

        info = {}
        for folder in os.listdir(self.path):
            folder_path = os.path.join(self.path, folder) 
            if os.path.isdir(folder_path):
                # file all file .yml in folder
                for file in os.listdir(folder_path):
                    if file.endswith('.yml'):
                        file_path = os.path.join(folder_path, file)
                        # read content of file .yml
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = yaml.safe_load(f)
                            for enti in content['models']:
                                name = str(enti['name']).replace(' ', '_').lower()
                                info[f"{name}"] = []
                                # split column for all model
                                for columns in enti['columns']:
                                    column_name = columns['name']
                                    if column_name not in ['load_date', 'record_source'] and column_name.endswith('hash_key'):
                                        info[f"{name}"].append(column_name)

        # check if existing model are in use
        hub_detail_info = {"model_name": [], "column_name": []}
        link_detail_info = {"model_name": [], "column_name": []}
        satellite_detail_info = {"model_name": [], "column_name": []}

        for model, cloumn_names in info.items():
            if model.startswith('hub'):
                for entity in cloumn_names:
                    hub_detail_info["model_name"].append(model)
                    hub_detail_info["column_name"].append(entity)
            elif model.startswith('link'):
                for entity in cloumn_names:
                    link_detail_info["model_name"].append(model)
                    link_detail_info["column_name"].append(entity)
            elif model.startswith('satellite'):
                for entity in cloumn_names:
                    satellite_detail_info["model_name"].append(model)
                    satellite_detail_info["column_name"].append(entity)

        print(hub_detail_info)
        print(link_detail_info)
        print(satellite_detail_info)

        # create dict to save info of all hub link satellite
        info_all = {"hub_satellite": {"source": hub_detail_info, "target": satellite_detail_info}, "hub_link": {"source": hub_detail_info, "target": link_detail_info}, "link_satellite": {"source": link_detail_info, "target": satellite_detail_info}}

        # check mapping of all model, check mapping new and insert it into table of link in meta vault
        for key, data in info_all.items():
            check_mapping_new_of_link_table = f"""
                insert into dp_warehouse.meta_vault.link_{key}
                -- info of hub model
                with table_1_src as (
                    SELECT 
                        {key.split("_")[0]}_name,
                        column_name
                    FROM 
                        UNNEST(
                            zip (
                                ARRAY{data["source"]["model_name"]},
                                ARRAY{data["source"]["column_name"]}
                                )
                        ) AS t({key.split("_")[0]}_name, column_name)
                ),
                
                -- info of satellite model
                table_2_src as (
                    SELECT 
                        {key.split("_")[1]}_name,
                        column_name
                    FROM 
                        UNNEST(
                            zip (
                                ARRAY{data["target"]["model_name"]},
                                ARRAY{data["target"]["column_name"]}
                                )
                        ) AS t({key.split("_")[1]}_name, column_name)
                ),
                
                src as (
                    select 
                        a.{key.split("_")[0]}_name,
                        b.{key.split("_")[1]}_name
                    FROM table_1_src a
                    JOIN table_2_src b
                    ON a.column_name = b.column_name
                )

                select 
                    s.{key.split("_")[0]}_name,
                    s.{key.split("_")[1]}_name,
                    1
                FROM src s
                LEFT JOIN dp_warehouse.meta_vault.link_{key} t
                ON s.{key.split("_")[0]}_name = t.{key.split("_")[0]}_name and s.{key.split("_")[1]}_name = t.{key.split("_")[1]}_name
                WHERE t.{key.split("_")[0]}_name is NULL and t.{key.split("_")[1]}_name is NULL
            """


            # execute all scripts above
            cur.execute(check_mapping_new_of_link_table)
            conn.commit()

        # check mapping of all model, to update activa status of all table link of meta vault
        for key, data in info_all.items():

            # update table link when find mapping not have in model
            check_active_status_when_not_have_model_link_of_data_vault = f"""
            UPDATE dp_warehouse.meta_vault.link_{key}
            SET active_status = 0
            WHERE ({key.split("_")[0]}_name, {key.split("_")[1]}_name) in (
                select 
                    target.{key.split("_")[0]}_name,
                    target.{key.split("_")[1]}_name
                FROM 
                    (select 
                        h.{key.split("_")[0]}_name,
                        s.{key.split("_")[1]}_name
                    FROM (
                        SELECT 
                            {key.split("_")[0]}_name,
                            column_name
                        FROM 
                            UNNEST(
                                zip (
                                    ARRAY{data["source"]["model_name"]},
                                    ARRAY{data["source"]["column_name"]}
                                    )
                            ) AS t({key.split("_")[0]}_name, column_name)
                    ) h
                    JOIN (
                        SELECT 
                            {key.split("_")[1]}_name,
                            column_name
                        FROM 
                            UNNEST(
                                zip (
                                    ARRAY{data["target"]["model_name"]},
                                    ARRAY{data["target"]["column_name"]}
                                    )
                            ) AS t({key.split("_")[1]}_name, column_name)
                    ) s
                    ON h.column_name = s.column_name
                    ) source
                RIGHT JOIN dp_warehouse.meta_vault.link_{key} target
                ON source.{key.split("_")[0]}_name = target.{key.split("_")[0]}_name AND source.{key.split("_")[1]}_name = target.{key.split("_")[1]}_name
                WHERE source.{key.split("_")[0]}_name is NULL
            )
            """

            cur.execute(check_active_status_when_not_have_model_link_of_data_vault)
            conn.commit()

            # update table link when find mapping have in model
            check_active_status_when_have_model_link_of_data_vault = f"""
            UPDATE dp_warehouse.meta_vault.link_{key}
            SET active_status = 1
            WHERE ({key.split("_")[0]}_name, {key.split("_")[1]}_name) in (
                select 
                    source.{key.split("_")[0]}_name,
                    source.{key.split("_")[1]}_name
                FROM 
                    (select 
                        h.{key.split("_")[0]}_name,
                        s.{key.split("_")[1]}_name
                    FROM (
                        SELECT 
                            {key.split("_")[0]}_name,
                            column_name
                        FROM 
                            UNNEST(
                                zip (
                                    ARRAY{data["source"]["model_name"]},
                                    ARRAY{data["source"]["column_name"]}
                                    )
                            ) AS t({key.split("_")[0]}_name, column_name)
                    ) h
                    JOIN (
                        SELECT 
                            {key.split("_")[1]}_name,
                            column_name
                        FROM 
                            UNNEST(
                                zip (
                                    ARRAY{data["target"]["model_name"]},
                                    ARRAY{data["target"]["column_name"]}
                                    )
                            ) AS t({key.split("_")[1]}_name, column_name)
                    ) s
                    ON h.column_name = s.column_name
                    ) source
                JOIN dp_warehouse.meta_vault.link_{key} target
                ON source.{key.split("_")[0]}_name = target.{key.split("_")[0]}_name AND source.{key.split("_")[1]}_name = target.{key.split("_")[1]}_name
            )
            """

            cur.execute(check_active_status_when_have_model_link_of_data_vault)
            conn.commit()

    # Create function to check and update mapping of satellite table in metavault
    def get_satellite_entity(self):
        conn = self._get_connection()
        cur = conn.cursor()

        info = {'hub':{'yaml': [], 'model': []}, 'link':{'yaml': [], 'model': []}, 'satellite':{'yaml': [], 'model': []}}

        for folder in os.listdir(self.path):
            folder_path = os.path.join(self.path, folder)
            if os.path.isdir(folder_path):  # Kiểm tra xem có phải là folder không
                # Tìm tất cả các file .yml trong folder
                for file in os.listdir(folder_path):
                    if file.endswith('.yml'):
                        file_path = os.path.join(folder_path, file)
                        # Đọc nội dung file .yml
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = yaml.safe_load(f)
                            for enti in content['models']:
                                name = str(enti['name']).replace(' ', '_').lower()
                                if name.startswith('hub'):
                                    # data in file.yaml
                                    info['hub']['yaml'].append(f'{name}')
                                elif name.startswith('link'):
                                    # data in file.yaml
                                    info['link']['yaml'].append(f'{name}')
                                elif name.startswith('satellite'):
                                    # data in file.yaml 
                                    info['satellite']['yaml'].append(f'{name}')
                    else:
                        file_path = os.path.join(folder_path, file)
                        for model in os.listdir(file_path):
                            name = str(model).split('.')[0].lower()
                            if name.startswith('hub'):
                                # data in file.yaml
                                info['hub']['model'].append(f'{name}')
                            elif name.startswith('link'):
                                # data in file.yaml
                                info['link']['model'].append(f'{name}')
                            elif name.startswith('satellite'):
                                # data in file.yaml 
                                info['satellite']['model'].append(f'{name}')

        # update info of table satellite by file.yaml and model in dbt
        for entity, values in info.items():
            # get values insert with new model
            query_insert = f"""
            INSERT INTO dp_warehouse.meta_vault.satellite_{entity} ({entity}_name, create_date, active_status)
            SELECT 
                s.{entity}_name,
                cast(cast(current_timestamp as date) as varchar),
                1
            FROM (
                SELECT 
                    {entity}_name
                FROM 
                    UNNEST(
                            ARRAY{values['yaml']}
                    ) AS t({entity}_name)
                ) s
            LEFT JOIN ( select * from dp_warehouse.meta_vault.satellite_{entity}
                        where retire_date is NULL) t
            ON s.{entity}_name = t.{entity}_name
            WHERE t.{entity}_name is NULL
            """
            cur.execute(query_insert)
            conn.commit()

            # check and update status retire date of model
            check_update_model = f"""
            UPDATE dp_warehouse.meta_vault.satellite_{entity}
            SET retire_date = cast(cast(current_timestamp as date) as varchar), active_status = 0
            WHERE ({entity}_name) in (
                SELECT 
                    t.{entity}_name
                FROM (
                    SELECT 
                        {entity}_name
                    FROM 
                        UNNEST(
                                ARRAY{values['yaml']}
                        ) AS t({entity}_name)
                    ) s
                RIGHT JOIN dp_warehouse.meta_vault.satellite_{entity} t
                ON s.{entity}_name = t.{entity}_name
                WHERE s.{entity}_name is NULL
            )
            """
            cur.execute(check_update_model)
            conn.commit()

            # Check mapping between file.yaml and model in dbt project to update in satellite table
            check_model_of_dbt_when_match = f"""
            UPDATE dp_warehouse.meta_vault.satellite_{entity}
            SET in_use = 1
            WHERE ({entity}_name) in (
                SELECT 
                    t.{entity}_name
                FROM (
                    SELECT 
                        {entity}_name
                    FROM 
                        UNNEST(
                                ARRAY{values['model']}
                        ) AS t({entity}_name)
                    ) s
                JOIN dp_warehouse.meta_vault.satellite_{entity} t
                ON s.{entity}_name = t.{entity}_name
            )
            """
            cur.execute(check_model_of_dbt_when_match)
            conn.commit()

            check_model_of_dbt_when_not_match = f"""
            UPDATE dp_warehouse.meta_vault.satellite_{entity}
            SET in_use = 0
            WHERE ({entity}_name) in (
                SELECT 
                    t.{entity}_name
                FROM (
                    SELECT 
                        {entity}_name
                    FROM 
                        UNNEST(
                                ARRAY{values['model']}
                        ) AS t({entity}_name)
                    ) s
                RIGHT JOIN dp_warehouse.meta_vault.satellite_{entity} t
                ON s.{entity}_name = t.{entity}_name
                WHERE s.{entity}_name is NULL
            )
            """
            cur.execute(check_model_of_dbt_when_not_match)
            conn.commit()

    # Create function to check and update detail of satellite_satellite_detail table in metavault
    def get_satellite_satellite_detail_entity(self):
        conn = self._get_connection()
        cur = conn.cursor()

        info = {}
        for folder in os.listdir(self.path):
            folder_path = os.path.join(self.path, folder) 
            if os.path.isdir(folder_path):
                # file all file .yml in folder
                for file in os.listdir(folder_path):
                    if file.endswith('.yml'):
                        file_path = os.path.join(folder_path, file)
                        # read content of file .yml
                        with open(file_path, 'r', encoding='utf-8') as f:
                            content = yaml.safe_load(f)
                            for enti in content['models']:
                                name = str(enti['name']).replace(' ', '_').lower()
                                info[f"{name}"] = []
                                # split column for all model
                                for columns in enti['columns']:
                                    column_name = columns['name']
                                    if column_name not in ['load_date', 'record_source', 'load_end_date']:
                                        info[f"{name}"].append(column_name)

        # check if existing model are in use
        satellite_detail_info = {"model_name": [], "column_name": []}

        for model, cloumn_names in info.items():
            if model.startswith('satellite'):
                for entity in cloumn_names:
                    satellite_detail_info["model_name"].append(model)
                    satellite_detail_info["column_name"].append(entity)

        print(satellite_detail_info)

        # Check new column for table satellite
        check_new_model_satellite = f"""
            INSERT INTO dp_warehouse.meta_vault.satellite_satellite_detail (satellite_name, satellite_change_date, attribute_name, change_type, change_description)
            
            -- check new table to insert
            with new_table as (
                select
                    s.satellite_name,
                    s.column_name
                from
                    (
                    SELECT 
                        satellite_name,
                        column_name
                    FROM 
                        UNNEST(
                            zip (
                                ARRAY{satellite_detail_info["model_name"]},
                                ARRAY{satellite_detail_info["column_name"]}
                                )
                        ) AS t(satellite_name, column_name)
                    ) s
                LEFT JOIN dp_warehouse.meta_vault.satellite_satellite_detail t
                ON s.satellite_name = t.satellite_name
                WHERE t.satellite_name is NULL
            )

            SELECT 
                satellite_name,
                cast(cast(current_timestamp as date) as varchar),
                column_name,
                NULL,
                NULL
            FROM new_table
        """

        cur.execute(check_new_model_satellite)
        conn.commit()

        # check attribute_name of every model satellite, check model satellite be added new column
        check_new_column_satellite = f"""
            INSERT INTO dp_warehouse.meta_vault.satellite_satellite_detail (satellite_name, satellite_change_date, attribute_name, change_type, change_description)
            
            -- check new column to insert 
            with new_column as (
                select
                    s.satellite_name,
                    s.column_name
                from
                    (
                    SELECT 
                        satellite_name,
                        column_name
                    FROM 
                        UNNEST(
                            zip (
                                ARRAY{satellite_detail_info["model_name"]},
                                ARRAY{satellite_detail_info["column_name"]}
                                )
                        ) AS t(satellite_name, column_name)
                    ) s
                LEFT JOIN dp_warehouse.meta_vault.satellite_satellite_detail t
                ON s.satellite_name = t.satellite_name and s.column_name = t.attribute_name
                WHERE t.attribute_name is NULL
            )

            SELECT 
                satellite_name,
                cast(cast(current_timestamp as date) as varchar),
                column_name,
                'add',
                NULL
            FROM new_column
        """

        cur.execute(check_new_column_satellite)
        conn.commit()

        # check attribute_name of every model satellite, check model satellite be deleted old column
        check_delete_column_satellite = f"""
            UPDATE dp_warehouse.meta_vault.satellite_satellite_detail
            SET satellite_change_date = cast(cast(current_timestamp as date) as varchar), change_type = 'delete'
            WHERE (satellite_name, attribute_name) in (
                select 
                    t.satellite_name,
                    t.attribute_name
                from dp_warehouse.meta_vault.satellite_satellite_detail t
                LEFT JOIN (
                    SELECT 
                        satellite_name,
                        column_name
                    FROM 
                        UNNEST(
                            zip (
                                ARRAY{satellite_detail_info["model_name"]},
                                ARRAY{satellite_detail_info["column_name"]}
                                )
                        ) AS t(satellite_name, column_name) 
                ) s
                ON s.satellite_name = t.satellite_name and s.column_name = t.attribute_name
                WHERE s.column_name is NULL
            )
        """

        cur.execute(check_delete_column_satellite)
        conn.commit()

        # check attribute_name of every model satellite, check model satellite be reused with old column
        check_reused_column_satellite = f"""
            UPDATE dp_warehouse.meta_vault.satellite_satellite_detail
            SET satellite_change_date = cast(cast(current_timestamp as date) as varchar), change_type = 'reused'
            WHERE (satellite_name, attribute_name) in (
                select 
                    t.satellite_name,
                    t.attribute_name
                from dp_warehouse.meta_vault.satellite_satellite_detail t
                JOIN (
                    SELECT 
                        satellite_name,
                        column_name
                    FROM 
                        UNNEST(
                            zip (
                                ARRAY{satellite_detail_info["model_name"]},
                                ARRAY{satellite_detail_info["column_name"]}
                                )
                        ) AS t(satellite_name, column_name) 
                ) s
                ON s.satellite_name = t.satellite_name and s.column_name = t.attribute_name
                WHERE t.change_type = 'delete'
            )
        """

        cur.execute(check_reused_column_satellite)
        conn.commit()