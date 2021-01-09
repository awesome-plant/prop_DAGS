#site bits 
# import requests
# from lxml.html import fromstring
# import pandas as pd
import mods.pre_flight as pf 
#airflow bits
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

def PreFlight(): 
    print("starting preflight")
    df_proxies, pfp_status=pf.preFlightProxy(5)
    if pfp_status==True:
        npl_status=pf.newProxyList(df_proxies)
        if npl_status==True: 
            print("imported new proxies successfully")

default_args={
    'owner': 'Airflow'
    ,'start_date': datetime.datetime.now() - datetime.timedelta(days=1) #yesterday
    }

preflight_proxy = DAG(
        dag_id='preflight_proxy'
        ,default_args=default_args
        ,schedule_interval='@hourly'
        ,start_date=days_ago(1)
        ,tags=['preflight_proxy']
        ,catchup=False
    )
preflight_starter = DummyOperator( dag = preflight_proxy, task_id='dummy_starter' )
preflight_ender = DummyOperator( dag = preflight_proxy, task_id='dummy_ender' )
scrape_task = PythonOperator(
    task_id="scrape_preflight"
    ,provide_context=True
    ,python_callable=PreFlight
    ,dag = preflight_proxy
    )
preflight_starter >> scrape_task >> preflight_ender 