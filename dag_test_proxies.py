#test proxies in db 
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import mods.db_import as db_import
import logging
import datetime
from airflow import models
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.utils.dates import days_ago
from airflow import DAG



volume_mount = VolumeMount(
    'persist-airflow-logs'
    , mount_path='/opt/airflow/logs'
    , sub_path=None
    , read_only=False
    )

volume_config= {
    'persistentVolumeClaim': { 'claimName': 'persist-airflow-logs' }
    }

volume = Volume(
    name='persist-airflow-logs'
    , configs=volume_config
    )

default_args = {
    'owner': 'airflow',
    }

#get current proxies in db
import mods.proxy as proxy
import math
batch_size=100
proxy_count=proxy.getProxyCount(ps_user="postgres", ps_pass="root", ps_host="172.22.114.65", ps_port="5432", ps_db="scrape_db")

with DAG(
    dag_id='dag_get_proxies',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    sitemap_starter = DummyOperator(task_id='dummy_starter' )
    sitemap_ender = DummyOperator(task_id='dummy_ender' )
    for sql_start in range(0, math.ceil(proxy_count/batch_size)):
        proxy_test = KubernetesPodOperator(
            namespace='airflow'
            , name="proxies-test_b_" + str(sql_start)
            , task_id="proxies-test_b_" + str(sql_start)
            , image="babadillo12345/airflow-plant:scrape_worker-1.1"
            , cmds=["bash", "-cx"]
            , arguments=["git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/proxy.py check_Proxy " + str(sql_start) + str(batch_size)]  
            , image_pull_policy="IfNotPresent"
            , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
            , labels={"foo": "bar"}
            , volumes=[volume]
            , volume_mounts=[volume_mount]
            , is_delete_operator_pod=True
            , in_cluster=True
            )
        sitemap_starter >> proxy_test >> sitemap_ender

# for i in range(0, math.ceil(proxy_count/batch_size)):
#     print("""
#     SELECT proxy
#     FROM sc_land.sc_proxy_raw 
#     limit """ + str(batch_size) + """ 
#     offset """ + str(i*batch_size) + """ 
#     """)


# with DAG(
#     dag_id='dag_test_proxies',
#     default_args=default_args,
#     schedule_interval=None,
#     start_date=days_ago(1),
#     tags=['example'],
# ) as dag:
#     openproxy = KubernetesPodOperator(
#         namespace='airflow'
#         , name="proxies-openproxy"
#         , task_id="proxies-openproxy"
#         , image="babadillo12345/airflow-plant:scrape_worker-1.1"
#         , cmds=["bash", "-cx"]
#         , arguments=["git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/proxy.py openproxy"]  
#         , image_pull_policy="IfNotPresent"
#         , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
#         , labels={"foo": "bar"}
#         , volumes=[volume]
#         , volume_mounts=[volume_mount]
#         , is_delete_operator_pod=True
#         , in_cluster=True
#         )
#     proxyscrape = KubernetesPodOperator(
#         namespace='airflow'
#         , name="proxies-proxyscrape"
#         , task_id="proxies-proxyscrape"
#         , image="babadillo12345/airflow-plant:scrape_worker-1.1"
#         , cmds=["bash", "-cx"]
#         , arguments=["git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/proxy.py proxyscrape"]  
#         , image_pull_policy="IfNotPresent"
#         , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
#         , labels={"foo": "bar"}
#         , volumes=[volume]
#         , volume_mounts=[volume_mount]
#         , is_delete_operator_pod=True
#         , in_cluster=True
#         )
#     proxy_list = KubernetesPodOperator(
#         namespace='airflow'
#         , name="proxies-proxy_list"
#         , task_id="proxies-proxy_list"
#         , image="babadillo12345/airflow-plant:scrape_worker-1.1"
#         , cmds=["bash", "-cx"]
#         , arguments=["git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/proxy.py proxy_list"]  
#         , image_pull_policy="IfNotPresent"
#         , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
#         , labels={"foo": "bar"}
#         , volumes=[volume]
#         , volume_mounts=[volume_mount]
#         , is_delete_operator_pod=True
#         , in_cluster=True
#         )
#     proxynova = KubernetesPodOperator(
#         namespace='airflow'
#         , name="proxies-proxynova"
#         , task_id="proxies-proxynova"
#         , image="babadillo12345/airflow-plant:scrape_worker-1.1"
#         , cmds=["bash", "-cx"]
#         , arguments=["git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/proxy.py proxynova"]  
#         , image_pull_policy="IfNotPresent"
#         , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
#         , labels={"foo": "bar"}
#         , volumes=[volume]
#         , volume_mounts=[volume_mount]
#         , is_delete_operator_pod=True
#         , in_cluster=True
#         )
#     sitemap_starter = DummyOperator(task_id='dummy_starter' )
#     sitemap_ender = DummyOperator(task_id='dummy_ender' )
# sitemap_starter >> openproxy >> sitemap_ender 
# sitemap_starter >> proxy_list >> sitemap_ender 
# sitemap_starter >> proxynova >> sitemap_ender 
# sitemap_starter >> proxyscrape >> sitemap_ender 