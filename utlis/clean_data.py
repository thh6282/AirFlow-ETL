from dags.utils.etl_manager import AirflowETLManager
from trino.dbapi import connect
from dags.utils.variables import trino_catalog
from concurrent.futures import ThreadPoolExecutor
import sys
from functools import wraps
import logging

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
def optimize_table(cur, schema_name, table_name):
    """
    Optimize and clean up a table by adjusting file size thresholds and removing orphan files.
    
    :param cur: The cursor object to execute database queries.
    :param schema_name: The name of the schema containing the table to be optimized.
    :param table_name: The name of the table to be optimized.
    """
    
    # Establish a new connection to the database
    conn = etl_manager._get_connection()
    cur = conn.cursor()

    # Execute the optimize command to adjust file size thresholds
    cur.execute(f"""ALTER TABLE {trino_catalog}.{schema_name}."{table_name}" EXECUTE optimize(file_size_threshold => '500MB')""")
    
    # Execute the remove orphan files command to clean up orphan files
    cur.execute(f"""ALTER TABLE {trino_catalog}.{schema_name}."{table_name}" EXECUTE remove_orphan_files(retention_threshold => '30d')""")
    
    # Log the successful optimization of the table
    logging.info(f"Table {trino_catalog}.{schema_name}.{table_name} has been optimized")
    
    # Close the cursor
    cur.close()
    
@stop_on_error
def housekeeping__clean_data_file() -> None:
    """
    The `clean_data_file` function optimizes tables in a Trino catalog by adjusting file size thresholds
    and removing orphan files.
    """
    
    # Set up logging configuration
    logging.basicConfig(level=logging.INFO)
    
    # Define the catalog and schemas to be processed
    catalog = trino_catalog
    schemas = ["operational_metadata", "staging", "raw_vault", "check", "meta_vault"]

    # Establish a connection to the database
    conn = etl_manager._get_connection()
    cur = conn.cursor()

    # Query to fetch all base tables excluding those in the information_schema
    query = f"SELECT table_schema, table_name FROM {catalog}.information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema <> 'information_schema'"
    cur.execute(query)
    tables = cur.fetchall()

    # Append a new column to each table entry for indexing
    for index, table in enumerate(tables):
        new_column = f"{table[0]}_{index}"
        table.append(new_column)

    # Use ThreadPoolExecutor to optimize tables concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for schema_name, table_name, cur in tables:
            if schema_name in schemas:
                futures.append(executor.submit(optimize_table, cur, schema_name, table_name))

    # Commit the transaction
    conn.commit()
    
    # Log the successful completion of the clean data file process
    logging.info("Clean data file successfully")
    
# write main function to run the task
# if __name__ == "__main__":
#     housekeeping__clean_data_file()