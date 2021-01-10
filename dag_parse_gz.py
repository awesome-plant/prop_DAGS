# from lxml import etree
import os
# import time
import datetime 
import pandas as pd
# import numpy as np
# import mods.proxy as proxy 
# import requests
# import gzip
# import shutil
# import psycopg2
# from sqlalchemy import create_engine
# from psycopg2 import Error

#everything specific here needed 
import math  
#airflow libs
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

batch_size=20
_max_name=''
_max_path=''
_max_mod= 0

def print_list(scrape_batch):
    print(scrape_batch)

# https://stackoverflow.com/questions/52558018/airflow-generate-dynamic-tasks-in-single-dag-task-n1-is-dependent-on-taskn
for x in os.scandir('/opt/airflow/logs/XML_save_folder/raw_sitemap'):
    if os.path.getmtime(x.path) > _max_mod:
        _max_name=x.name
        _max_path=x.path
        _max_mod=os.path.getmtime(x.path)
XML_H_Dataset= pd.read_csv(_max_path) 

default_args={
    'owner': 'Airflow'
    ,'start_date': datetime.datetime.now() - datetime.timedelta(days=1) #yesterday
    }


xml_parse_dag = DAG(
        dag_id='xml_parse_dag'
        ,default_args=default_args
        ,schedule_interval=None
        # ,start_date=days_ago(1)
        ,tags=['get_xml_parse']
    )
for i in range(0, math.ceil(XML_H_Dataset[XML_H_Dataset['filetype'].notnull()].shape[0]/batch_size)):
    # print(XML_H_Dataset[XML_H_Dataset['filetype'].notnull()]['s_filename'].iloc[(i*batch_size)-batch_size:(i*batch_size) + 1])
    xml_gz_extract=PythonOperator(
            task_id='scrape_sitemap_batch_'+str(i)
            ,provide_context=True
            ,op_kwargs={
                'scrape_batch': XML_H_Dataset[XML_H_Dataset['filetype'].notnull()]['s_filename'].iloc[(i*batch_size)-batch_size:(i*batch_size) + 1]
            #     'baseurl': 'https://www.realestate.com.au/xml-sitemap/'
            #     , 'PageSaveFolder': '/opt/airflow/logs/XML_save_folder/gz_files/'
            #     , 'ScrapeFile': XML_H_Dataset[XML_H_Dataset['filetype'].notnull()]['s_filename'].iloc[i:i + 1].to_string(index=False).strip() #pass in filename from filtered iteration
            #     ,'Scrapewait': 5
                }
            ,python_callable=print_list #SaveScrape
            ,dag=xml_parse_dag
            )



xml_parse_starter = DummyOperator( dag = xml_parse_dag, task_id='dummy_starter' )
xml_parse_ender = DummyOperator( dag = xml_parse_dag, task_id='dummy_ender' )

