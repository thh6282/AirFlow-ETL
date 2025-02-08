from datetime import datetime
from trino.dbapi import connect
from airflow.models import Variable
from collections import defaultdict
from dags.utils.etl_manager import AirflowETLManager

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from dags.utils.variables import ETL_DATE

etl_manager = AirflowETLManager()

conn = etl_manager._get_connection()
cur = conn.cursor()
etl_date = ETL_DATE

# list value for insert into table check_num_of_records
values_list = []

sources = [
    '101', '102', '200', '230', '600', '888',
    '420', '210', '220', '402', '405', '411', 
    '466', '468', '488', '501', '668', '500', 
    'booking', 'extappdata'
    ]
    
source_exact = [
'101', '102', '200', '230', '600','888'
]

source_nhamay = [
'210', '220', '402', '405', '411', '420', 
'466', '468', '488', '501', '668']

table_name_exact = [   
'cicmpy',
'cicntp',
'divisioncreditors',
'divisiondebtors',
'addresses',
'humres',
'hrjbtl',
'hrloc',
'pwrole',
'kstpl',
'kstdr',
'fatran',
'grtbk',
'gbkmut',
'dagbk',
'ksprek',
'ksdrek',
'items',
'staffl',
'itemclasses',
'itemassortment',
'itemnumbers',
'stfoms',
'recipe',
'orhkrg',
'orhsrg',
'frkrg',
'frsrg',
'magaz',
'evloc',
'pwruc',
'ordlev',
'betcd',
'voorrd',
'itemaccounts',
'fagrp',
'orkrg',
'orsrg'
]

table_name_exact_inc = [
    'gbkmut',
    'orhkrg',
    'orhsrg',
    'orkrg',
    'orsrg',
    'staffl',
    'voorrd'
]



table_name_appdata = [
'appusers',
'dmssellout',
'dms_orderdetail',
'dms_ordermaster',
'ecompromo',
'ecompromodetail',
'ecompromodetailprice',
'ecompromofactor',
'mpldeliveryfee',
'mpldeliveryfeecalculator',
'mpldeliveryfeecalculatorbyitem',
'mpldeliverymanager',
'mpldeliverypreload',
'mpldeliverypreloaddetail',
'mpldeliverypreloaditem',
'mplvehicles',
'promo',
'promodetail',
'promodetailbytype',
'promodetailresult',
'shmaragency',
'shmaragencycampaign',
'shmarcampaign',
'shmartradesaleoffresult',
'shmartradesaleoffresultdetail',
'shshopkey',
'smpromoprogram',
'smpromoprogramdetail',
'smpromoprogramdetailresult',
'wh_location',
'wh_location_user',
'wh_rack',
'wh_rack_balancehistory',
'wh_rack_item'
]

table_name_appdata_inc = [
    'dmssellout',
    'mpldeliveryfeecalculatorbyitem',
    'mpldeliverymanager',
    'mpldeliverypreloaddetail',
    'mpldeliverypreloaditem',
    'smpromoprogramdetail',
    'wh_rack_balancehistory'
]

def create_query(source:str, table_name:str) -> tuple[str, str]:
    """Create query to get number of records in staging and source table"""
    if source == '500':
        if table_name in table_name_exact_inc:
            query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_exact{source}__{table_name}"""
            query_source = f"""SELECT count(*) FROM dp_src_nhamay.dbo.src_{source}_{table_name}"""
        else:
            query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_exact{source}__{table_name} WHERE load_date = '{etl_date}'"""
            query_source = f"""SELECT count(*) FROM dp_src_nhamay.dbo.src_{source}_{table_name}"""
    elif source in source_exact:
        if source == '101':
            if table_name in table_name_exact_inc:
                if table_name == 'orsrg' or table_name == 'orkrg':
                    query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_exact{source}__{table_name}"""
                    query_source = f"""SELECT count(*) FROM dp_src_nhamay.dbo.src_rep_101_{table_name}"""
                else:
                    query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_exact{source}__{table_name}"""
                    query_source = f"""SELECT count(*) FROM dp_src_exact{source}.dbo.{table_name}"""
            else:
                query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_exact{source}__{table_name} WHERE load_date = '{etl_date}'"""
                query_source = f"""SELECT count(*) FROM dp_src_exact{source}.dbo.{table_name}"""
        else: 
            if table_name in table_name_exact_inc:
                    query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_exact{source}__{table_name}"""
                    query_source = f"""SELECT count(*) FROM dp_src_exact{source}.dbo.{table_name}"""
            else:
                query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_exact{source}__{table_name} WHERE load_date = '{etl_date}'"""
                query_source = f"""SELECT count(*) FROM dp_src_exact{source}.dbo.{table_name}"""
    elif source in source_nhamay:
        if table_name in table_name_exact_inc:
            query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_nhamay{source}__{table_name}"""
            query_source = f"""SELECT count(*) FROM dp_src_nhamay.dbo.src_{source}_{table_name}"""
        else:
            query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_nhamay{source}__{table_name} WHERE load_date = '{etl_date}'"""
            query_source = f"""SELECT count(*) FROM dp_src_nhamay.dbo.src_{source}_{table_name}"""
    elif source == 'booking':
        query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_appdatashg{source}__{table_name_appdata[0]} WHERE load_date = '{etl_date}'"""
        query_source = f"""SELECT count(*) FROM dp_src_shg.dbo.src_shg_{source}_{table_name_appdata[0]}"""
    else:
        if table_name in table_name_appdata_inc:
            query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_appdatashg{source}__{table_name}"""
            query_source = f"""SELECT count(*) FROM dp_src_shg.dbo.src_shg_{source}_{table_name}"""
        else:
            query_stg = f"""SELECT count(*) FROM dp_warehouse.staging.stg_appdatashg{source}__{table_name} WHERE load_date = '{etl_date}'"""
            query_source = f"""SELECT count(*) FROM dp_src_shg.dbo.src_shg_{source}_{table_name}"""  
    return query_stg, query_source

def excute_query(source:str, table_name:str) -> tuple[int, int]:
    """excute query to get number of records in staging and source table"""
    query_count = create_query(source, table_name)
    query_stg = query_count[0]
    query_source = query_count[1]
    
    cur.execute(query_stg)
    num_of_records_staging = cur.fetchall()[0][0]
    
    cur.execute(query_source)
    num_of_records_source = cur.fetchall()[0][0]
    
    return num_of_records_staging, num_of_records_source

def query_value_list(source:str, table_name:str) -> None:
    """query value to insert into table check_num_of_records"""
    result_query = excute_query(source, table_name)
    num_of_records_staging = result_query[0]  
    num_of_records_source = result_query[1]          
    value_str = f"('{etl_date}', '{source}', '{table_name}', {num_of_records_staging}, {num_of_records_source} )"
    values_list.append(value_str)
    
    
def check_num_of_records() -> None:
    """Insert number of records in staging and source table into table check_num_of_records"""
    query = f"""INSERT INTO dp_warehouse."check".check_num_of_records VALUES """

    for source in sources:
        if source == '500':
            for table_name in table_name_exact:
                query_value_list(source, table_name)
        elif source in source_exact:
            for table_name in table_name_exact:
                query_value_list(source, table_name)
        elif source in source_nhamay:
            for table_name in table_name_exact:
                query_value_list(source, table_name)
        elif source == 'booking':
                query_value_list(source, table_name_appdata[0])
        else:
            for table_name in table_name_appdata:
                query_value_list(source, table_name)
        
    query += ", ".join(values_list)
        
    cur.execute(query)
    conn.commit()
    cur.close()

