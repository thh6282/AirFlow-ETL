from dags.utils.etl_manager import AirflowETLManager
from datetime import datetime, timedelta
from airflow.models import Variable
from typing import Dict, List
import pandas as pd

from dags.utils.variables import ETL_DATE, trino_catalog, trino_schema

catalog = trino_catalog
schema = trino_schema

etl_manager = AirflowETLManager()

"""group function to check and get content email when schema change"""

def content_email(date:str, source_name: str):
    """Function to get content email when schema change"""
    # TODO: why run SELECT query here?
    # configuration to trino
    conn = etl_manager._get_connection()
    cur = conn.cursor()
    
    # get run_id in current_day
    run_id = etl_manager._get_run_id(source_name)

    table = "schemadrift_tablechangedetail"
    # Execute the query to get all tables in the specified catalog and schema
    query = f"""
    SELECT run_id, table_name, source_name, column_name, data_type, status, change_date, created_date
    FROM {catalog}.{schema}.{table}
    WHERE run_id  = '{run_id}'
    order by table_name, column_name
    """
    
    # Fetch the results
    try:
        cur.execute(query)
        tables = cur.fetchall()
        conn.commit()
    except:
        tables = []

    table_change = "schemadrift_tablechange"
    # Execute the query to get all tables in the specified catalog and schema
    query = f"""
    SELECT run_id, table_name, source_name, is_updated
    FROM {catalog}.{schema}.{table_change}
    WHERE run_id = '{run_id}'
    """

    try:
        cur.execute(query)
        table_change = cur.fetchall()
        conn.commit()
    except:
        table_change = []
    
    # read data to data frame
    df = pd.DataFrame(tables, columns=['run_id', 'table_name', 'source_name', 'column_name', 'data_type', 'status', 'change_date', 'created_date'])

    # filter data
    df_deleted = df[df['status'] == 'D'][['run_id', 'table_name', 'source_name', 'column_name', 'data_type', 'change_date', 'created_date']]
    df_added = df[df['status'] == 'I'][['run_id', 'table_name', 'source_name', 'column_name', 'data_type', 'change_date', 'created_date']]
    

    return df_deleted, df_added, table_change, run_id


def generate_html(df_deleted, df_added) -> str:
    """
    Generates an HTML email template with the provided DataFrames for deleted and added records.
    This function converts the given DataFrames into HTML tables and inserts them into an email template
    fetched from a database. The template is customized to reflect schema changes.
    Args:
        df_deleted (pd.DataFrame): DataFrame containing records that have been deleted.
        df_added (pd.DataFrame): DataFrame containing records that have been added.
    Returns:
        str: The HTML content of the email template with the inserted DataFrame tables. 
             Returns an empty string if no template is found.
    """
    """ Function to get email template and return content html if schema change """
    # Use Pandas to convert DataFrame to HTML
    html_deleted = df_deleted.to_html(index=False, classes='deleted')
    html_added = df_added.to_html(index=False, classes='added')

    # configuration to trino
    conn = etl_manager._get_connection()

    cur = conn.cursor()
    
    table = "email_template"
    # Execute the query to get template email when schema change
    query = f"""
    SELECT distinct template
    FROM {catalog}.{schema}.{table}
    WHERE "group" = 'SCHEMA_DRIFT'
    
    """
    try:
        cur.execute(query)
        email_template = cur.fetchall()
        conn.commit()
    except:
        email_template = []

    if email_template != []:
        email_template = (email_template[0][0]
                            .replace("{html_deleted}", html_deleted)
                            .replace("{html_added}" , html_added))
    else:
        email_template = ''
    
    return email_template


def update_email_notification_status(source_name: str) -> None:
    """
    Updates the email notification status in the database if there is a schema change.
    This function retrieves the necessary data for the email notification, generates the email content,
    and inserts a record into the email_notification table for each recipient.
    Args:
        source_name (str): The name of the source database.
    Returns:
        None
    """
    """Funtion to excute update email notification if schema change"""
    conn = etl_manager._get_connection()
    cur = conn.cursor()

    df_deleted, df_added, table_change, run_id = content_email(ETL_DATE, source_name)
    
    # recipient email to list object
    to_email = get_recipient_email_list_schema_change()
    
    subject = f'Schema change report for the source database on {ETL_DATE}'
    html = generate_html(df_deleted, df_added)
    for i in to_email:
        sql = f"""insert into {catalog}.{schema}.email_notification (
                            run_id,
                            recipient_email,
                            event_type,
                            status,
                            email_subject,
                            email_body,
                            created_at,
                            created_by,
                            etl_date
                        ) values
                        (  
                            '{run_id}',
                            '{i}',
                            'SCHEMA_DRIFT',
                            'Succcess',
                            '{subject}',
                            '{html}',
                            current_timestamp,
                            'airflow',
                            '{ETL_DATE}' 
                            )"""
        cur.execute(sql) 


def get_content_email_schema_change(source_name: str) -> str:
    """
    Generate the content for an email notification when a schema change is detected.

    Args:
        source_name (str): The name of the data source where the schema change occurred.

    Returns:
        str: The HTML content for the email notification.
    """
    """Function to get content email to give send email task when schema change"""
    df_deleted, df_added, table_change, run_id = content_email(ETL_DATE, source_name)
    html = generate_html(df_deleted, df_added)    
    return html

def get_recipient_email_list_schema_change() -> list:
    """
    Retrieves a list of recipient email addresses for sending notifications when a schema change occurs.
    This function connects to the database, executes a query to fetch email addresses from the 
    'email_recipient_group' table where the group is 'SCHEMA_DRIFT', and returns the list of email addresses.
    Returns:
        list: A list of email addresses to notify about schema changes.
    """
    """Function to get recipient_email to give send email task when schema change"""

    # Execute the query to get all recipient_email when schema change
    conn = etl_manager._get_connection()
    cur = conn.cursor()
    email_list_table = 'email_recipient_group' 
    query = f"""
    SELECT recipient_email
    FROM {catalog}.{schema}.{email_list_table}
    WHERE "group" = 'SCHEMA_DRIFT'
    """
    try:
        cur.execute(query)
        recipient_email_list = cur.fetchall()
        conn.commit()
    except:
        recipient_email_list = []

    to_email = []
    for i in recipient_email_list:
        to_email.append(i[0])    
    return to_email


def check_schema_change(source_name: str) -> str:
    """
    Checks for schema changes in the data source.

    This function determines if there have been any schema changes in the specified data source
    by comparing the added and deleted data frames and the table change list. If there are any
    changes, it returns the email list for schema drift notifications; otherwise, it returns 'end'.

    Args:
        source_name (str): The name of the data source to check for schema changes.

    Returns:
        str: The email list for schema drift notifications if changes are detected, otherwise 'end'.
    """
    """Function to check schema change"""
    df_deleted, df_added, table_change, run_id = content_email(ETL_DATE, source_name)
    print(table_change)
    if (len(df_added) > 0 or len(df_deleted) > 0 ) and table_change != []:
        return 'schema_drift_notification.recipient_email_list_schema_drift'
    else:
        return 'end'



def get_email_template_pipeline_completed() -> str:
    """
    Fetches the email template for when the ETL pipeline is completed.
    This function connects to the database, executes a query to retrieve the email 
    template associated with the 'ETL_COMPLETE' group, and returns the template as 
    an HTML string. If the query fails or no template is found, an empty string is returned.
    Returns:
        str: The HTML content of the email template or an empty string if not found.
    """
    """Function to get email template when etl pipeline completed"""
    conn = etl_manager._get_connection()
    cur = conn.cursor()

    query = f"""SELECT template 
                FROM {catalog}.{schema}.email_template
                WHERE "group" = 'ETL_COMPLETE'"""
    try:
        cur.execute(query)
        # Fetch the results
        temp = cur.fetchall()
        conn.commit()
    except:
        temp = []

    if temp != []:
        html = temp[0][0]
    else:
        html = ""

    return html


def get_recipient_email_list_etl_completed() -> List[str]:
    """
    Function to get recipient email list when ETL pipeline is completed.
    This function connects to a Trino database, executes a query to fetch 
    email addresses from the 'email_recipient_group' table where the group 
    is 'ETL_COMPLETE', and returns the list of email addresses.
    Returns:
        List[str]: A list of recipient email addresses.
    """
    """Function to get recipient email list when etl pipeline completed"""
    # connect to trino
    conn = etl_manager._get_connection()
    cur = conn.cursor()
    to_email = []

    # query
    email_list_table = 'email_recipient_group' 
    query = f"""
                SELECT recipient_email
                FROM {catalog}.{schema}.{email_list_table}
                WHERE "group" = 'ETL_COMPLETE'
                """
    try:
        cur.execute(query)
        recipient_email_list = cur.fetchall()
        conn.commit()
    except:
        recipient_email_list = []
    # convert to list
    for i in recipient_email_list:
        to_email.append(i[0])

    return to_email
    


def update_email_notification_etl_completed_for_all_source() -> None:
    """
    Updates the email notification status in the database when the ETL pipeline is completed for all sources.
    This function performs the following steps:
    1. Establishes a connection to the ETL manager.
    2. Retrieves the list of recipient email addresses for ETL completion notifications.
    3. Generates the email subject and body content for the ETL completion notification.
    4. Inserts a record into the email_notification table for each recipient, indicating that the ETL process has completed successfully.
    Note:
    - The function assumes the existence of certain helper functions and constants such as 
      `get_recipient_email_list_etl_completed`, `generate_content_email_etl_completed_for_all_source`, 
      and `ETL_DATE`.
    - The database schema and catalog are assumed to be defined elsewhere in the code.
    Raises:
    - Any exceptions raised by the database connection or cursor operations will propagate up to the caller.
    """
    """Function to update mail sending status when etl pipeline completed for all source"""

    conn = etl_manager._get_connection()
    cur = conn.cursor()


    recipient_email_list = get_recipient_email_list_etl_completed()
    email_subject = "ETL Pipeline Status"
    event_type = "ETL_COMPLETED"
    email_body = generate_content_email_etl_completed_for_all_source()
    etl_date = ETL_DATE


    for i in recipient_email_list:
        sql = f"""insert into {catalog}.{schema}.email_notification (
                            run_id,
                            recipient_email,
                            event_type,
                            status,
                            email_subject,
                            email_body,
                            created_at,
                            created_by,
                            etl_date
                        ) values
                        (  
                            '{etl_date}',
                            '{i}',
                            '{event_type}',
                            'Succcess',
                            '{email_subject}',
                            '{email_body}',
                            current_timestamp,
                            'airflow',
                            '{ETL_DATE}' 
                            )"""
        cur.execute(sql) 
        conn.commit()







def get_param_etl_pipeline() -> dict:
    """
    Function to get all parameters (staging_total, datavault_total, staging_success, datavault_success, staging_failure, datavault_failure) 
    to send an email when the ETL pipeline is completed for all sources.
    Returns:
        dict: A dictionary where each key is a source name and the value is another dictionary containing:
            - staging_total (int): Total number of staging tables for the source.
            - datavault_total (int): Total number of raw vault tables for the source.
            - staging_success (int): Total number of successful staging tables for the source.
            - datavault_success (int): Total number of successful raw vault tables for the source.
            - staging_failure (int): Total number of failed staging tables for the source.
            - datavault_failure (int): Total number of failed raw vault tables for the source.
    """
    """Function to get all params (staing_total, datavault_total, staging_success, datavault_success, staging_failure, datavault_failure) to send email when etl pipeline completed for all source"""
    conn = etl_manager._get_connection()
    cur = conn.cursor()
    
    sources_name = [
    'appdatashgbooking',
    'appdatashgextappdata',
    'exact101',
    'exact102',
    'exact200',
    'exact230',
    'exact500',
    'exact600',
    'exact888',
    'nhamay210',
    'nhamay220',
    'nhamay402',
    'nhamay405',
    'nhamay411',
    'nhamay420',
    'nhamay466',
    'nhamay468',
    'nhamay488',
    'nhamay501',
    'nhamay668'
    ]

    etl_pipeline_params_table = 'checkpoint_etlpipeline_dbt_metadata'

    dict = {}   
    etl_date = ETL_DATE


    for source_name in sources_name:

        query_run_id = f"""SELECT run_id 
                        FROM dp_warehouse.operational_metadata.info_etlpipeline_by_sources
                        WHERE etl_date = '{etl_date}'
                        AND source_name = '{source_name}'
                        AND created_at = (
                            SELECT MAX(created_at)
                            FROM dp_warehouse.operational_metadata.info_etlpipeline_by_sources
                            WHERE etl_date = '{etl_date}' AND source_name = '{source_name}'
                        )"""
        try:
            cur.execute(query_run_id)
            run_id = cur.fetchall()[0][0]
        except:
            run_id = ""
        

        
        # get the total number of staging tables of each source pipeline
        # query
        query = f"""
                    SELECT count(distinct name)
                    FROM {catalog}.{schema}.{etl_pipeline_params_table}
                    WHERE run_id = '{run_id}' AND schema_name = 'staging' and source_name = '{source_name}' and etl_date = '{etl_date}'

                    """
        try:
            cur.execute(query)
            staging_total = cur.fetchall()
            conn.commit()
        except:
            staging_total = []

        if staging_total != []:
            staging_total = staging_total[0][0]
        else:
            staging_total = 0
        
        # get the total number of rawvault tables of each source pipeline
        # query
        query = f"""
                    SELECT count(distinct name)
                    FROM {catalog}.{schema}.{etl_pipeline_params_table}
                    WHERE run_id = '{run_id}' AND schema_name = 'raw_vault' and source_name = '{source_name}' and etl_date = '{etl_date}'
                    """
        try:
            cur.execute(query)
            datavault_total = cur.fetchall()
            conn.commit()
        except:
            datavault_total = []

        if datavault_total != []:
            datavault_total = datavault_total[0][0]
        else:
            datavault_total = 0

        # get the total number of successful staging tables of pipeline
        # query
        query = f"""
                    SELECT count(distinct name)
                    FROM {catalog}.{schema}.{etl_pipeline_params_table}
                    WHERE run_id = '{run_id}' AND schema_name = 'staging' AND status = 'success' and source_name = '{source_name}' and etl_date = '{etl_date}'
                    """
        try:
            cur.execute(query)
            staging_success = cur.fetchall()
            conn.commit()
        except:
            staging_success = []

        if staging_success != []:
            staging_success = staging_success[0][0]
        else:
            staging_success = 0

        # get the total number of successful raw vault tables of pipeline
        # query
        query = f"""
                    SELECT count(distinct name)
                    FROM {catalog}.{schema}.{etl_pipeline_params_table}
                    WHERE run_id = '{run_id}' AND schema_name = 'raw_vault' AND status = 'success' and source_name = '{source_name}' and etl_date = '{etl_date}'
                    """
        try:
            cur.execute(query)
            datavault_success = cur.fetchall()
            conn.commit()
        except:
            datavault_success = []

        if datavault_success != []:
            datavault_success = datavault_success[0][0]
        else:
            datavault_success = 0

        # failure = total - success
        staging_failure = staging_total - staging_success
        datavault_failure = datavault_total - datavault_success

        dictionary = {}
        dictionary['staging_total'] = staging_total
        dictionary['datavault_total'] = datavault_total
        dictionary['staging_success'] = staging_success
        dictionary['datavault_success'] = datavault_success
        dictionary['staging_failure'] = staging_failure
        dictionary['datavault_failure'] = datavault_failure

        dict[source_name] = dictionary
    
    return dict
    

def get_content_summary_template() -> str:
    """
    Generates an HTML summary template for the ETL pipeline stages.
    This function retrieves ETL pipeline parameters and calculates the total,
    successful, and failed counts for both the Staging and Data Vault stages.
    It then formats these values into an HTML template and returns the formatted string.
    Returns:
        str: An HTML string containing the summary of the ETL pipeline stages.
    """
    """Function to return template summary params etl pipeline"""
    dict = get_param_etl_pipeline()
    summary_format = """
        <th>Stage</th> 
            <th>Total Tables</th> 
            <th>Successful</th> 
            <th>Failed</th> 
        </tr> 
        </thead> 
        <tbody> 
        <tr> 
            <td>Staging</td> 
            <td>{{total_staging_total}}</td> 
            <td class="success">{{total_staging_success}}</td> 
            <td class="failure">{{total_staging_failure}}</td> 
        </tr> 
        <tr> 
            <td>Data Vault</td> 
            <td>{{total_datavault_total}}</td> 
            <td class="success">{{total_datavault_success}}</td> 
            <td class="failure">{{total_datavault_failure}}</td>
        """
    
    total_staging_total = 0
    total_datavault_total = 0
    total_staging_success = 0
    total_datavault_success = 0
    total_staging_failure = 0
    total_datavault_failure = 0 

    for key in dict:
        total_staging_total += dict[key]['staging_total']
        total_datavault_total += dict[key]['datavault_total']
        total_staging_success += dict[key]['staging_success']
        total_datavault_success += dict[key]['datavault_success']
        total_staging_failure += dict[key]['staging_failure']
        total_datavault_failure += dict[key]['datavault_failure']

    summary_format = (summary_format.replace('{{total_staging_total}}', str(total_staging_total))
                .replace('{{total_datavault_total}}', str(total_datavault_total))
                .replace('{{total_staging_success}}', str(total_staging_success))
                .replace('{{total_datavault_success}}', str(total_datavault_success))
                .replace('{{total_staging_failure}}', str(total_staging_failure))
                .replace('{{total_datavault_failure}}', str(total_datavault_failure))
                )
    
    return summary_format


def get_content_detail_template() -> str:
    """
    Function to return template detail parameters for the ETL pipeline.
    This function retrieves ETL pipeline parameters and generates HTML table rows
    for both staging and data vault processes. It uses predefined HTML templates
    and replaces placeholders with actual values from the ETL pipeline parameters.
    Returns:
        tuple: A tuple containing two strings:
            - staging_string: HTML table rows for staging process details.
            - vault_string: HTML table rows for data vault process details.
    """
    """Function to return template detail params etl pipeline"""
    dict = get_param_etl_pipeline()

    staging_string = ""
    vault_string = ""

    template_staging = """<tr>
                                <td>Staging</td>
                                <td>{{source}}</td>
                                <td>{{totalTables}}</td>
                                <td class="success">{{stagingsuccessful}}</td>
                                <td class="failure">{{stagingfailed}}</td>
                            </tr>"""
    template_vault =  """  <tr>
                                <td>Data vault</td>
                                <td>{{source}}</td>
                                <td>{{totalvault}}</td>
                                <td class="success">{{vaultsuccessful}}</td>
                                <td class="failure">{{vaultfailed}}</td>
                            </tr>"""

    for key, value in dict.items():
        stg = (template_staging.replace('{{source}}', key)
                        .replace('{{totalTables}}', str(value['staging_total']))
                        .replace('{{stagingsuccessful}}', str(value['staging_success']))
                        .replace('{{stagingfailed}}', str(value['staging_failure']))
        )

        vault =     (template_vault.replace('{{source}}', key)
                        .replace('{{totalvault}}', str(value['datavault_total']))
                        .replace('{{vaultsuccessful}}', str(value['datavault_success']))
                        .replace('{{vaultfailed}}', str(value['datavault_failure']))
        )    
        staging_string += stg
        vault_string += vault

    return staging_string, vault_string

def generate_content_email_etl_completed_for_all_source() -> str:
    """
    Generates the HTML content for an email notification when the ETL pipeline 
    has completed for all sources.
    This function retrieves the email template for a completed pipeline, 
    inserts a summary of the ETL process, and includes detailed information 
    about the staging and vault processes. It also replaces placeholders 
    with the actual ETL date.
    Returns:
        str: The HTML content for the email notification.
    """
    """Function to get content email when etl pipeline completed for all source"""
    html = ""
    html_temp = get_email_template_pipeline_completed()
    summary = get_content_summary_template()
    staging, vault = get_content_detail_template()
    
    html = (html_temp.replace('{{data_sumary}}', summary)
                .replace('{{data_detail_staging}}', staging)
                .replace('{{data_detail_vault}}', vault)
                .replace('{{etl_date}}', ETL_DATE)
            )
    return html


def check_etl_pipeline_error() -> str:
    """
    Checks for errors in the ETL pipeline by examining the failure counts in the 
    staging and datavault stages.

    Returns:
        str: The string 'send_email_etl_error.recipient_email_list' if there are 
        any failures in either the staging or datavault stages, otherwise 'end'.
    """
    dictionary = get_param_etl_pipeline()
    for key in dictionary:
        if dictionary[key]['staging_failure'] > 0 or dictionary[key]['datavault_failure'] > 0:
            return 'send_email_etl_error.recipient_email_list'
    return 'end'


def get_email_template_pipeline_error() -> str:
    """
    Function to get the email template for ETL pipeline errors.
    This function retrieves the email template from the database when an ETL pipeline error occurs.
    It connects to the database, executes a query to fetch the template associated with the 'ETL_ERROR' group,
    and returns the template as a string. If no template is found, it returns an empty string.
    Returns:
        str: The email template for ETL pipeline errors, or an empty string if no template is found.
    """ 
    """Function to get email template when etl pipeline completed"""
    
    conn = etl_manager._get_connection()
    cur = conn.cursor()
    
    query = f"""SELECT template 
                FROM {catalog}.{schema}.email_template
                WHERE "group" = 'ETL_ERROR'"""
    try:
        cur.execute(query)
        # Fetch the results
        temp = cur.fetchall()
        conn.commit()
    except:
        temp = []

    if temp != []:
        html = temp[0][0]
    else:
        html = ""

    return html


def get_params_detail_etl_error() -> List[str]:
    """
    Retrieves detailed ETL error information for various data sources.
    This function connects to the ETL database, queries for the latest run_id for each source,
    and retrieves detailed error information for each source based on the run_id. The results
    are transformed and aggregated into an HTML string format for reporting purposes.
    Returns:
        List[str]: A list containing:
            - total_stg_src_error (str): Total number of staging source errors.
            - total_dv_src_error (str): Total number of datavault source errors.
            - string (str): HTML string containing detailed error information for each source.
    """

    conn = etl_manager._get_connection()
    cur = conn.cursor()

    sources_name = [
    'appdatashgbooking',
    'appdatashgextappdata',
    'exact101',
    'exact102',
    'exact200',
    'exact230',
    'exact500',
    'exact600',
    'exact888',
    'nhamay210',
    'nhamay220',
    'nhamay402',
    'nhamay405',
    'nhamay411',
    'nhamay420',
    'nhamay466',
    'nhamay468',
    'nhamay488',
    'nhamay501',
    'nhamay668'
    ]

    etl_pipeline_params_table = 'checkpoint_etlpipeline_dbt_metadata'

    dict = {}   
    etl_date = ETL_DATE

    string = ""
    total_stg_src_error = 0
    total_dv_src_error = 0
    df = pd.DataFrame()
    for source_name in sources_name:
        # get run_id of each source
        query_run_id = f"""SELECT run_id 
                        FROM dp_warehouse.operational_metadata.info_etlpipeline_by_sources
                        WHERE etl_date = '{etl_date}'
                        AND source_name = '{source_name}'
                        AND created_at = (
                            SELECT MAX(created_at)
                            FROM dp_warehouse.operational_metadata.info_etlpipeline_by_sources
                            WHERE etl_date = '{etl_date}' AND source_name = '{source_name}'
                        )"""
        try:
            cur.execute(query_run_id)
            run_id = cur.fetchall()[0][0]
            dict[source_name] = run_id
        except:
            run_id = ""
        # get detail error of each source using run_id
        query_detail_error = f"""SELECT  source_name, dag_id, task_id, start_time, end_time
                                from dp_warehouse.operational_metadata.status_etlpipeline_task_airflow 
                                where status not in ('success', 'skipped') and run_id = '{run_id}'"""
        try:
            cur.execute(query_detail_error)
            result = cur.fetchall()

             # convert to dataframe and transform data
            result = pd.DataFrame(result, columns=['source_name', 'dag_id', 'task_id', 'start_time', 'end_time'])
            result['start_time'] = result['start_time'] + timedelta(hours=7)
            result['end_time'] = result['end_time'] + timedelta(hours=7)
            result['dag_id'] = result['dag_id'].str.split('__', expand=True)[2]

            df = pd.concat([df, result], axis=0)
        except:
            result = pd.DataFrame()
        
       

      
    template = """<tr>
                        <td>{{source}}</td>
                        <td>{{dag_id}}</td>
                        <td>{{task_id}}</td>
                        <td >{{start_time}}</td>
                        <td >{{end_time}}</td>
                    </tr>"""

    # loop dataframe
    for index, row in df.iterrows():
        string += template.replace("{{source}}", row['source_name']).replace("{{dag_id}}", row['dag_id']).replace("{{task_id}}", row['task_id']).replace("{{start_time}}", str(row['start_time'])).replace("{{end_time}}", str(row['end_time']))

    try:
        total_stg_src_error = str(df.groupby('dag_id').count().loc['staging']['source_name'])
    except:
        total_stg_src_error = '0'

    try:
        total_dv_src_error = str(df.groupby('dag_id').count().loc['datavault']['source_name'])
    except:
        total_dv_src_error = '0'

    return total_stg_src_error, total_dv_src_error, string



def generate_content_email_etl_pipeline_error() -> str:
    """
    Generates the HTML content for an ETL pipeline error email.
    This function retrieves an email template for pipeline errors and populates it
    with details about the errors encountered during the ETL process. The details
    include the total number of errors in the staging and vault layers, a detailed
    error message, and the ETL date.
    Returns:
        str: The HTML content for the ETL pipeline error email.
    """

    html = ""
    html_temp = get_email_template_pipeline_error()
    total_stg_src_error, total_dv_src_error, string = get_params_detail_etl_error()
    html = (html_temp.replace('{{total_staging}}', str(total_stg_src_error))
                .replace('{{total_vault}}', str(total_dv_src_error))
                .replace('{{data_detail_error}}', string)
                .replace('{{etl_date}}', ETL_DATE)
            )
    
    return html




def update_email_notification_etl_error() -> None:
    """
    Updates the email notification status in the database when an ETL pipeline error occurs.
    This function performs the following steps:
    1. Establishes a connection to the ETL manager's database.
    2. Retrieves the list of recipient email addresses for ETL completion notifications.
    3. Defines the email subject, event type, and email body content for the ETL error notification.
    4. Inserts a new record into the email_notification table for each recipient email address, 
       indicating that an ETL error has occurred.
    Note:
        - The function assumes the existence of certain helper functions and variables such as 
          `get_recipient_email_list_etl_completed`, `generate_content_email_etl_pipeline_error`, 
          and `ETL_DATE`.
        - The database schema and catalog are assumed to be defined elsewhere in the code.
    Raises:
        Any exceptions raised by the database connection or cursor operations will propagate up to the caller.
    """
    """Function to update mail sending status when etl pipeline error"""

    conn = etl_manager._get_connection()
    cur = conn.cursor()


    recipient_email_list = get_recipient_email_list_etl_completed()
    email_subject = "ETL Pipeline ERROR"
    event_type = "ETL_ERROR"
    email_body = generate_content_email_etl_pipeline_error()
    etl_date = ETL_DATE


    for i in recipient_email_list:
        sql = f"""insert into {catalog}.{schema}.email_notification (
                            run_id,
                            recipient_email,
                            event_type,
                            status,
                            email_subject,
                            email_body,
                            created_at,
                            created_by,
                            etl_date
                        ) values
                        (  
                            '{etl_date}',
                            '{i}',
                            '{event_type}',
                            'Succcess',
                            '{email_subject}',
                            '{email_body}',
                            current_timestamp,
                            'airflow',
                            '{ETL_DATE}' 
                            )"""
        cur.execute(sql) 
        conn.commit()
