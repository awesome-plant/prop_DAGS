#test proxies in db 
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import mods.db_import as db_import
import mods.proxy as proxy
import math
import logging
import datetime
from airflow import models
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
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
    'owner': 'airflow'
    ,'retries': 1
}

#get current proxies in db
batch_size=150
batch_g_size=10 #used to remove that pod timeout error 
proxy_count=proxy.getProxyCount(ps_user="postgres", ps_pass="root", ps_host="172.22.114.65", ps_port="5432", ps_db="scrape_db")

with DAG(
    dag_id='2.dag_test_proxies',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    group=1 
    count=0 
    # split_old = DummyOperator(task_id='dummy_starter',trigger_rule='all_done')
    split_old = KubernetesPodOperator(
                namespace='airflow'
                , name="refresh_ip_" + str(0)
                , task_id="refresh_ip_" + str(0)
                , image="babadillo12345/airflow-plant:scrape_worker-1.2"
                , cmds=["bash", "-cx"]
                , arguments=["unset https_proxy && git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/proxy.py -mod refresh_ip"]  
                , image_pull_policy="IfNotPresent"
                , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
                , labels={"foo": "bar"}
                , volumes=[volume]
                , volume_mounts=[volume_mount]
                , is_delete_operator_pod=True
                , in_cluster=True
                ,trigger_rule='all_done'
            )
    split_new = KubernetesPodOperator(
                namespace='airflow'
                , name="refresh_ip_" + str(group)
                , task_id="refresh_ip_" + str(group)
                , image="babadillo12345/airflow-plant:scrape_worker-1.2"
                , cmds=["bash", "-cx"]
                , arguments=["unset https_proxy && git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/proxy.py -mod refresh_ip"]  
                , image_pull_policy="IfNotPresent"
                , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
                , labels={"foo": "bar"}
                , volumes=[volume]
                , volume_mounts=[volume_mount]
                , is_delete_operator_pod=True
                , in_cluster=True
                ,trigger_rule='all_done'
            )
    
    for sql_start in range(0, math.ceil(proxy_count/batch_size)):
        if count == batch_g_size:
            split_old = split_new
            count=0 
            group+=1
            # split_new = DummyOperator(task_id='dummy_splitter_' + str(group),trigger_rule='all_done')
            split_new = KubernetesPodOperator(
                namespace='airflow'
                , name="refresh_ip_" + str(group)
                , task_id="refresh_ip_" + str(group)
                , image="babadillo12345/airflow-plant:scrape_worker-1.2"
                , cmds=["bash", "-cx"]
                , arguments=["unset https_proxy && git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/proxy.py -mod refresh_ip"]  
                , image_pull_policy="IfNotPresent"
                , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
                , labels={"foo": "bar"}
                , volumes=[volume]
                , volume_mounts=[volume_mount]
                , is_delete_operator_pod=True
                , in_cluster=True
                ,trigger_rule='all_done'
            )
        proxy_test = KubernetesPodOperator(
                namespace='airflow'
                , name="proxies-test_b_" + str(sql_start)
                , task_id="proxies-test_b_" + str(sql_start)
                , image="babadillo12345/airflow-plant:scrape_worker-1.2"
                , cmds=["bash", "-cx"]
                , arguments=["unset all_proxy && unset ALL_PROXY && git clone https://github.com/awesome-plant/prop_DAGS.git && echo SELECT proxy FROM sc_land.sc_proxy_raw order by table_id limit " + str(batch_size) + " offset " + str(sql_start*batch_size) + " && python prop_DAGS/mods/proxy.py -mod check_Proxy -st " + str(sql_start*batch_size) + " -si " + str(batch_size)]  
                , image_pull_policy="IfNotPresent"
                , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
                , labels={"foo": "bar"}
                , volumes=[volume]
                , volume_mounts=[volume_mount]
                , is_delete_operator_pod=True
                , in_cluster=True
                ,trigger_rule='all_done'
            )
        split_old >> proxy_test >> split_new
        count+=1