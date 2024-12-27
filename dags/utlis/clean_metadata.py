from datetime import datetime
from trino.dbapi import connect
from minio import Minio
import json
from minio.error import S3Error
from dags.utils.etl_manager import AirflowETLManager
import sys
import logging
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Set, Dict
from dags.utils.variables import trino_catalog, trino_schema, minio_host, minio_port, minio_access_key, minio_secret_key, minio_bucket_name, ETL_DATE

# MinIO connection details from Airflow Variables


catalog = trino_catalog
schema = trino_schema

bucket_name = minio_bucket_name
etl_date = ETL_DATE

etl_manager = AirflowETLManager()

def stop_on_error(func):
    """
    The `stop_on_error` decorator stops the execution of a function if an error occurs within it.
    
    :param func: The `func` parameter in the `stop_on_error` decorator is a function that you want to
    decorate. The decorator `stop_on_error` is designed to catch any exceptions that occur when the
    decorated function `func` is called. If an exception occurs, the decorator will print an error
    message indicating
    :return: The `stop_on_error` function is a decorator that wraps another function. It catches any
    exceptions that occur when the wrapped function is called. If an exception is caught, it prints an
    error message and exits the program with a status code of 1. The `wrapper` function is being
    returned by the `stop_on_error` function.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Error in {func.__name__}: {e}")
            sys.exit(1)  # stop all function if a func error
    return wrapper

@stop_on_error
def fetch_metadata_location(cur, catalog, schema, table):
    """
    Helper function to fetch metadata locations for a given table.
    """
    conn = etl_manager._get_connection()
    cur = conn.cursor()
    metadata_query = f"""SELECT file FROM {catalog}.{schema}."{table}$metadata_log_entries" """
    cur.execute(metadata_query)
    metadata_location = cur.fetchall()
    cur.close()
    return metadata_location


@stop_on_error
def get_metadata_location(bucket_name: str, catalog: str) -> List[str]:
    """
    This Python function retrieves metadata locations from a specified catalog in Trino concurrently,
    filters out information_schema tables, and collects metadata locations from other tables.

    :param bucket_name: Name of the S3 bucket where the metadata files are stored.
    :param catalog: The Trino catalog from which metadata locations are to be retrieved.
    :param trino_host: Hostname of the Trino server.
    :param trino_port: Port number of the Trino server.
    :param trino_user: Username for Trino server authentication.
    :return: A list of metadata locations for files in the specified bucket from a catalog in Trino.
    """
    conn = etl_manager._get_connection()
    cur = conn.cursor()

    # Get all table names excluding information_schema
    table_query = f"SELECT table_schema, table_name FROM {catalog}.information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema <> 'information_schema'"
    cur.execute(table_query)
    tables = cur.fetchall()
    
    for index, table in enumerate(tables):
        new_column = f"{table[0]}_{index}"
        table.append(new_column)
    
    
    metadata_location_list = []
    # Use ThreadPoolExecutor to fetch metadata locations concurrently
    with ThreadPoolExecutor(max_workers=20) as executor:  # Adjust max_workers based on your environment
        future_to_metadata = {
            executor.submit(fetch_metadata_location, cur, catalog, schema, table): (fetch_metadata_location)
            for schema, table, cur in tables
        }

        for future in as_completed(future_to_metadata):

            results = future.result()
            if f's3://{bucket_name}/' in results[0][0]:
                element = results[0][0].replace(f's3://{bucket_name}/', '')
                metadata_location_list.append(element)
            elif f's3a://{bucket_name}/' in results[0][0]:
                element = results[0][0].replace(f's3a://{bucket_name}/', '')
                metadata_location_list.append(element)

    metadata_location_list = [i.split('/metadata/')[0] for i in metadata_location_list]
    
    conn.close()
    return metadata_location_list

@stop_on_error
def get_path_prefix(minio_client: Minio, bucket_name:str, prefix:str) -> Set[str]:
    """
    The function `get_path_prefix` retrieves unique directories under a specified prefix in an S3 bucket
    using a Minio client.
    
    :param minio_client: Minio client object used to interact with the Minio server
    :type minio_client: Minio
    :param bucket_name: The `bucket_name` parameter is a string that represents the name of the S3
    bucket from which you want to retrieve unique directories under a given prefix
    :type bucket_name: str
    :param prefix: The `prefix` parameter in the `get_path_prefix` function is used to specify the
    common prefix for the objects you want to retrieve from the S3 bucket. It helps in filtering the
    objects based on a specific path within the bucket
    :type prefix: str
    :return: A set of unique directories under a given prefix in an S3 bucket.
    """
    # List objects under the prefix
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    unique_directories = set()
    for obj in objects:
        relative_path = obj.object_name[len(prefix):].lstrip('/')  
        if '/' in relative_path: 
            subdirectory = relative_path.split('/')[0]  
            unique_directories.add(f"{prefix.rstrip('/')}/{subdirectory}") 
    return unique_directories

@stop_on_error
def delete_folder(bucket_name:str, folder_name:str) -> None:
    """
    This Python function deletes all objects under a specific folder in an S3 bucket using the Minio
    client.
    
    :param bucket_name: The `bucket_name` parameter in the `delete_folder` function refers to the name
    of the S3 bucket from which you want to delete objects under a specific folder. This parameter
    should be a string that represents the name of the S3 bucket in which the folder is located
    :type bucket_name: str
    :param folder_name: The `folder_name` parameter in the `delete_folder` function represents the name
    of the folder within the specified S3 bucket that you want to delete. This function uses the `Minio`
    client to connect to the Minio server and delete all objects under the specified folder in the S3
    bucket
    :type folder_name: str
    """

    minio_client = Minio(
        endpoint=f"""{minio_host}:{minio_port}""",
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=True
    )
    
    objects_to_delete = minio_client.list_objects(bucket_name, prefix=folder_name, recursive=True)
    for obj in objects_to_delete:
        minio_client.remove_object(bucket_name, obj.object_name)
        print(f"successfully delete {bucket_name}/{obj.object_name} ")
    print(f"successfully delete {bucket_name}/{folder_name} ")


@stop_on_error
def housekeeping__clean_unused_file() -> None:
    """
    The function `housekeeping__clean_unused_file` cleans files in S3 that are not listed in the catalog
    metadata.
    """

    prefix = ['sh-warehouse/operational_metadata', 'sh-warehouse/staging', 'sh-warehouse/raw_vault', 'sh-warehouse/meta_vault', 'sh-warehouse/check']
    
    minio_client = Minio(
        endpoint=f"""{minio_host}:{minio_port}""",
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=True
    )

    print('Start clean folder not in catalog')

    metadata_location_list = get_metadata_location(bucket_name, catalog)

    path_to_delete = []
    for i in prefix:
        path_prefix = get_path_prefix(minio_client, bucket_name, i)
        for path in path_prefix:
            if path not in metadata_location_list:
                path_to_delete.append(path)
                
    for path in path_to_delete:
        print(f"path_to_delete: {path}")

    for path in path_to_delete:
        delete_folder(bucket_name, path)  

    print('End clean clean unused files')


############################################################################################################################################
### clean metadata file
@stop_on_error
def check_status_housekeeping_data_files():
    conn = etl_manager._get_connection()
    cur = conn.cursor()
    
    
    current_month = datetime.now().strftime('%Y-%m')
    
    query_status_housekeeping_data_files =  f"""select dag_id from {trino_catalog}.operational_metadata.status_etlpipeline_task_airflow
                                                where dag_id = 'housekeeping__data_files' 
                                                and date_format(created_at, '%Y-%m') = '{current_month}'
                                                and status = 'success'"""
                                                
    status_housekeeping_data_files = cur.execute(query_status_housekeeping_data_files).fetchone()
    
    if status_housekeeping_data_files is not None:
        return 'housekeeping__metadata_files'
    else:
        return 'end'


@stop_on_error
def read_data(minio_client: Minio, bucket: str, object_name: str) -> dict:
    """
    The function `read_data` reads data from a file in an S3 bucket and returns it as a dictionary after
    parsing it as JSON.
    
    :param minio_client: A Minio client object used to interact with the Minio server
    :type minio_client: Minio
    :param bucket: The `bucket` parameter in the `read_data` function refers to the name of the S3
    bucket from which you want to read the data. It is a string that specifies the name of the bucket
    where the object is located
    :type bucket: str
    :param object_name: The `object_name` parameter in the `read_data` function refers to the name of
    the file or object that you want to read from the specified S3 bucket. This parameter should be a
    string that represents the name of the object you want to retrieve data from
    :type object_name: str
    :return: A dictionary containing the data read from the file in the specified S3 bucket.
    """
    data = minio_client.get_object(bucket, object_name)
    json_data = data.read()
    return json.loads(json_data)
    

@stop_on_error
def list_files_in_folder(minio_client: Minio, bucket_name: str, folder_path:str) -> list:
    """
    This Python function lists all files in a folder within an S3 bucket using the provided Minio
    client, bucket name, and folder path.
    
    :param minio_client: The `minio_client` parameter is an instance of the Minio client class, which is
    used to interact with a Minio server or any S3-compatible object storage service. It provides
    methods for performing operations like uploading, downloading, and listing objects in buckets
    :type minio_client: Minio
    :param bucket_name: The `bucket_name` parameter is a string that represents the name of the S3
    bucket where the folder is located
    :type bucket_name: str
    :param folder_path: The `folder_path` parameter in the `list_files_in_folder` function represents
    the path of the folder within the S3 bucket for which you want to list all files. It is used to
    specify the location within the bucket where the files are stored
    :type folder_path: str
    :return: a list of file names in the specified folder path within the given S3 bucket.
    """
    return [obj.object_name for obj in minio_client.list_objects(bucket_name, prefix=folder_path, recursive=True)]
    
    
@stop_on_error
def get_metadata_prefix(bucket_name: str, catalog: str, schemas: List[str]) -> List[str]:
    """
    This Python function retrieves metadata locations from a specified catalog in Trino concurrently,
    filters out information_schema tables, and collects metadata locations from other tables.

    :param bucket_name: Name of the S3 bucket where the metadata files are stored.
    :param catalog: The Trino catalog from which metadata locations are to be retrieved.
    :param trino_host: Hostname of the Trino server.
    :param trino_port: Port number of the Trino server.
    :param trino_user: Username for Trino server authentication.
    :return: A list of metadata locations for files in the specified bucket from a catalog in Trino.
    """
    
    conn = etl_manager._get_connection()
    cur = conn.cursor()

    # Get all table names excluding information_schema
    table_query = f"SELECT table_schema, table_name FROM {catalog}.information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema <> 'information_schema'"
    cur.execute(table_query)
    tables = cur.fetchall()
    
    for index, table in enumerate(tables):
        new_column = f"{table[0]}_{index}"
        table.append(new_column)
    
    
    metadata_location_list = []
    # Use ThreadPoolExecutor to fetch metadata locations concurrently
    with ThreadPoolExecutor(max_workers=20) as executor:  # Adjust max_workers based on your environment
        futures = []
        for schema, table, cur in tables:
            if schema in schemas:
                future = executor.submit(fetch_metadata_location, cur, catalog, schema, table)
                futures.append(future)
            
        for future in as_completed(futures):

            results = future.result()
            if f's3://{bucket_name}/' in results[0][0]:
                element = results[0][0].replace(f's3://{bucket_name}/', '')
                metadata_location_list.append(element)
            elif f's3a://{bucket_name}/' in results[0][0]:
                element = results[0][0].replace(f's3a://{bucket_name}/', '')
                metadata_location_list.append(element)

    metadata_prefixes = [i.split('/metadata/')[0] + '/metadata/' for i in metadata_location_list]
    
    conn.close()
    logging.info('End get metadata prefix.')
    return metadata_prefixes


@stop_on_error
def process_prefix(minio_client: Minio, bucket_name: str, prefix: str, current_month: str) -> List[str]:
    """
    Processes a single folder (prefix) and returns a list of metadata files that match the current month.
    
    :param bucket_name: Name of the S3 bucket where the metadata files are stored.
    :param prefix: The folder path (prefix) to process.
    :param current_month: The current month in 'YYYY-MM' format to filter the files by date.
    :return: A list of metadata file names matching the current month.
    """
    object_in_month_with_prefix = []
    objects = minio_client.list_objects(bucket_name, prefix, recursive=True)

    for obj in objects:
        file_date = obj.last_modified.strftime('%Y-%m')
        if file_date == current_month:
            object_in_month_with_prefix.append(obj.object_name)
    
    return (prefix, object_in_month_with_prefix)

@stop_on_error
def fetch_objects(minio_client: Minio, bucket_name: str, metadata_prefixes: List[str]) -> Dict[str, List[str]]:
    """
    This Python function retrieves metadata files for each folder in the folder_metadata list.
    
    :param bucket_name: Name of the S3 bucket where the metadata files are stored.
    :param catalog: The Trino catalog from which metadata locations are to be retrieved.
    :param folder_metadata: A list of folder paths for which metadata files are to be retrieved.
    :return: A dictionary where keys are folder paths and values are lists of metadata files for each folder.
    """
    object_in_month = {}
    current_month = datetime.now().strftime('%Y-%m')

    # Use ThreadPoolExecutor to process multiple prefixes concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(lambda prefix: process_prefix(minio_client, bucket_name, prefix, current_month), metadata_prefixes)

    for prefix, objects in results:
        object_in_month[prefix] = objects
        
    return object_in_month

@stop_on_error
def get_objects_to_keep(minio_client: Minio, object_in_month: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """
    The function `get_objects_to_keep` determines which objects to keep based on a given dictionary of
    objects in a month.
    
    :param minio_client: The `minio_client` parameter is an instance of the Minio client that is used to
    interact with a Minio server or object storage service. It is typically used to perform operations
    such as uploading, downloading, and listing objects in buckets
    :type minio_client: Minio
    :param object_in_month: `object_in_month` is a dictionary where the keys are prefixes and the values
    are lists of objects. The objects in these lists are from a specific month
    :type object_in_month: Dict[str, List[str]]
    :return: The function `get_objects_to_keep` returns a dictionary where the keys are prefixes and the
    values are lists of objects to keep. If the input list of objects for a prefix is not empty, it
    includes those objects in the dictionary. If the input list is empty, it retrieves a list of files
    in the folder with that prefix from the Minio client and includes those in the dictionary.
    """
    objects_to_keep = {}
    
    for prefix, objects in object_in_month.items():
        if objects != []:
            objects_to_keep[prefix] = objects
        else:
            ob_keep_not_in_month = list_files_in_folder(minio_client, bucket_name, prefix)
            objects_to_keep[prefix] = ob_keep_not_in_month
    logging.info('End get objects to keep.\n')
    return objects_to_keep


@stop_on_error
def delete_object(minio_client: Minio, bucket_name: str, object_name: str) -> None:
    minio_client.remove_object(bucket_name, object_name)
    print(f"successfully delete {bucket_name}/{object_name} ")


@stop_on_error
def delete_object_not_to_keep(minio_client: Minio, metadata_prefixes: List[str], objects_to_keep: Dict[str, List[str]], bucket_name:str) -> None:
    files_to_delete = set()  # Initialize outside the loop to accumulate files from all prefixes

    for prefix in metadata_prefixes:
        all_files = list_files_in_folder(minio_client, bucket_name, prefix)
        # Assuming `object_in_month` is a dictionary where each prefix keys to a list of files to retain
        files_to_retain = set(objects_to_keep.get(prefix, []))
        files_to_delete.update(set(all_files) - files_to_retain)
    
    with ThreadPoolExecutor(max_workers=10) as executor:
    # Tạo một future cho mỗi file cần xóa
        for file in files_to_delete:
            executor.submit(delete_object, minio_client, bucket_name, file)
            
    logging.info('End delete object not to keep.\n')



    
# convert timestamp from milliseconds to datetime
def convert_timestamp(timestamp_ms: int) -> datetime:
    """
    The function `convert_timestamp` converts a timestamp from milliseconds to a datetime object.
    
    :param timestamp_ms: The `timestamp_ms` parameter in the `convert_timestamp` function represents a
    timestamp value in milliseconds that you want to convert to a `datetime` object
    :type timestamp_ms: int
    :return: a datetime object that corresponds to the timestamp converted from milliseconds to seconds.
    """
    timestamp_s = timestamp_ms / 1000
    return datetime.fromtimestamp(timestamp_s)


@stop_on_error
def process_object_snapid_and_time(obj: object, minio_client: Minio, bucket_name: str) -> Optional[str]:
    if obj.endswith('metadata.json'):
        metadata_data = read_data(minio_client, bucket_name, obj)
        snapshot_id = metadata_data.get('current-snapshot-id')
        last_update_ms = metadata_data.get('last-updated-ms')
        formatted_date = convert_timestamp(last_update_ms)
        schema_name = obj.split('/')[-4]
        table_name = obj.split('/')[-3]
        table_name = "_".join(table_name.split('_')[:-1])
        metadata_file = f"s3://{bucket_name}/" + obj
        value_str = f"({snapshot_id}, '{table_name}', '{schema_name}', '{metadata_file}', CAST('{formatted_date}' AS TIMESTAMP(6)), CAST(current_timestamp AS TIMESTAMP(6)), 'Airflow')"
        return value_str
    return None

@stop_on_error
def insert_info_meta_and_snap_to_db(minio_client: Minio, objects_to_keep: Dict[str, List[str]], catalog: str, bucket_name: str) -> None:
    """
    Inserts metadata and snapshot information into the housekeeping_metadata_location table in the specified catalog.

    :param minio_client: Minio client object used to interact with the Minio server.
    :param objects_to_keep: Dictionary where keys are prefixes and values are lists of objects to keep.
    :param catalog: The Trino catalog where the housekeeping_metadata_location table is located.
    :param bucket_name: Name of the S3 bucket where the metadata files are stored.
    """
    query = f"INSERT INTO {catalog}.operational_metadata.housekeeping_metadata_location (snapshot_id, table_name, schema_name, metadata_location, snapshot_time, create_at, create_by) VALUES "

    values_list = []
    # Use ThreadPoolExecutor to process objects concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_obj = {executor.submit(process_object_snapid_and_time, obj, minio_client, bucket_name): obj for path, objects in objects_to_keep.items() for obj in objects}
        for future in as_completed(future_to_obj):
            result = future.result()
            if result:
                values_list.append(result)

    chunk_size = 500
    queries = []

    logging.info('Executing query insert metadata location to housekeeping_metadata_location ...')
    
    # Split value_list into chunks of 500 values each
    for i in range(0, len(values_list), chunk_size):
        query = f"""INSERT INTO dp_warehouse.operational_metadata.housekeeping_metadata_location VALUES """
        
        chunk = values_list[i:i + chunk_size]
        # Join values in the chunk to form a single query
        query += ", ".join(chunk)
        queries.append(query)

    # Execute each query chunk
    conn = etl_manager._get_connection()
    cur = conn.cursor()
    for query in queries:
        cur.execute(query)
    conn.commit()
    conn.close()  

    logging.info(f'Successfully inserted informative metadata and snapshot to table {catalog}.operational_metadata.housekeeping_metadata_location.\n')

@stop_on_error
def housekeeping__clean_metadata_file() -> None:
    """
    The function `housekeeping__clean_metadata_file` cleans metadata files that are not listed in the
    catalog metadata and expired files from S3.
    """
    logging.info('Start clean metadata file ...')
    
    minio_client = Minio(
        endpoint=f"""{minio_host}:{minio_port}""",
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=True
    )
    
    schemas = ['operational_metadata', 'meta_vault', 'check', 'staging', 'raw_vault']
    
    logging.info('Get metadata location ...')
    
    metadata_prefixes = get_metadata_prefix(bucket_name, catalog, schemas)
    
    object_in_month = fetch_objects(minio_client, bucket_name, metadata_prefixes)
    
    logging.info('Get objects to keep ...')
    objects_to_keep = get_objects_to_keep(minio_client, object_in_month)
    
    logging.info('Start delete object not to keep ...')
    delete_object_not_to_keep(minio_client, metadata_prefixes, objects_to_keep, bucket_name)
    
    logging.info('Start insert info meta and snap to db ...')
    insert_info_meta_and_snap_to_db(minio_client, objects_to_keep, catalog, bucket_name)
    
    logging.info('End clean metadata file.')