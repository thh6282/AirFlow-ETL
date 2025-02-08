from dags.utils.variables import minio_bucket_name, ETL_DATE, trino_catalog
from dags.utils.etl_manager import AirflowETLManager

etl_manager = AirflowETLManager()



def get_metadata_location(bucket_name: str, catalog: str) -> list[str]:
    """
    This Python function retrieves metadata locations from a specified catalog in Trino.
    
    :param bucket_name: The `bucket_name` parameter in the `get_metadata_location` function is used to
    specify the name of the bucket from which you want to retrieve metadata locations in Trino
    :type bucket_name: str
    :param catalog: The `catalog` parameter in the `get_metadata_location` function refers to the
    specific catalog in Trino from which you want to retrieve metadata locations. It is a string
    parameter that should be provided when calling the function to specify the catalog you are
    interested in
    :type catalog: str
    :return: The function `get_metadata_location` returns a list of metadata locations from the
    specified catalog in Trino.
    """
    """Retrieve metadata locations from specified catalog in Trino."""
    try: 
        # Connect to Trino
        conn = etl_manager._get_connection()   
        cur = conn.cursor()

        # select table name in db
        query = f"""select table_catalog, table_schema, table_name from {catalog}.information_schema.tables"""
        cur.execute(query)
        table_name = cur.fetchall()

        table = {}
        for item in table_name:
            key = f"{item[0]}.{item[1]}"
            value = item[2]
            if key in table:
                table[key].append(value)
            else:
                table[key] = [value]

        # except information_schema table
        table = {k:v for k,v in table.items() if 'information_schema' not in k}

        # select metadata location 
        metadata_log_entries = []
        for key, value in table.items():
            for v in value:
                query = f"""select file from {key}."{v}$metadata_log_entries" """
                cur.execute(query)
                metadata_log_entries.append(cur.fetchall())
        cur.close()

        metadata_location = []
        # collect metadata location list
        for i in metadata_log_entries:
            if '_scd' not in i[0][0]:
                metadata_location.append(i[0][0])
        
        return metadata_location
    
    except Exception as e:
        print(f'Error: {e}')


def backup_nessie_postgres() -> None:
    """
    The function `backup_nessie_postgres` backs up metadata locations from Nessie Postgres to a backup
    metadata location table.
    """
    try:
        print('Backup Nessie Postgres')
        print('Get metadata location ...')
        metadata_location = get_metadata_location(minio_bucket_name, trino_catalog)
        print('get metadata location done')
        value_list = []
        print('create query insert metadata location to backup_metadata_location ...')
        for i in metadata_location:
            table_name = i.split('/')[5]
            table_name = '_'.join(table_name.split('_')[:-1])
            schema_name = i.split('/')[4]
            table_location = f"""{i.split('/metadata')[0]}""" 
            
            metadata_file_name = i.split('/')[-1]
            
            register_query = f"""CALL {trino_catalog}.system.register_table (schema_name => ''{schema_name}'', table_name => ''{table_name}'', table_location => ''{table_location}'', metadata_file_name => ''{metadata_file_name}'');"""
            table_id = f"""to_hex(md5(to_utf8('{schema_name}_{table_name}')))"""
            value_str = f"""({table_id}, '{table_name}', '{schema_name}', '{i}', '{register_query}', '{ETL_DATE}', CAST(current_timestamp AS TIMESTAMP(6)))"""
            value_list.append(value_str)
        
        chunk_size = 100
        queries = []

        print('excute query insert metadata location to backup_metadata_location ...')
        # Split value_list into chunks of 100 values each
        for i in range(0, len(value_list), chunk_size):
            query = f"""INSERT INTO dp_warehouse.operational_metadata.backup_metadata_location VALUES """
            
            chunk = value_list[i:i + chunk_size]
            # Join values in the chunk to form a single query
            query += ", ".join(chunk)
            queries.append(query)

        conn = etl_manager._get_connection()
        cur = conn.cursor()
        for query in queries:
            cur.execute(query)
        conn.commit()  
        cur.close()

        print('Backup Nessie Postgres done')
        
    except Exception as e:
        print(f'Error: {e}')
