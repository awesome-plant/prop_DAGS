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
# from kubernetes.client import models as k8s

default_args={
    'owner': 'Airflow'
    ,'start_date': datetime.datetime.now() - datetime.timedelta(days=1) #yesterday
    }


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
batch_size=15
batch_g_size=5 #used to remove that pod timeout error 
CP_count=db_import.getChildPagesCount(ps_user="postgres", ps_pass="root", ps_host="172.22.114.65", ps_port="5432", ps_db="scrape_db")

#iterate to run 
l_proxy_mods=["scrape_child"]

with DAG(
    dag_id='4.dag_get_sitemap_child',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    group=1 
    count=0 
    split_old = DummyOperator(task_id='dummy_starter',trigger_rule='all_done')
    split_new = DummyOperator(task_id='dummy_splitter_' + str(0),trigger_rule='all_done')
    for mod in l_proxy_mods:
        for sql_start in range(0, math.ceil(CP_count/batch_size)):
            if count == batch_g_size:
                split_old = split_new
                count=0 
                group+=1
                split_new = DummyOperator(task_id='dummy_splitter_' + str(group),trigger_rule='all_done')

            smp_mod = KubernetesPodOperator(
                namespace='airflow'
                , name="get_smp-" + mod + "_" + str(sql_start)
                , task_id="get_smp-" + mod + "_" + str(sql_start)
                , image="babadillo12345/airflow-plant:scrape_worker-1.2"
                , cmds=["bash", "-cx"]
                , arguments=["git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/realestate.py -mod " + mod + " -st " + str(batch_size*sql_start) + " -si " + str(batch_size)]  
                , image_pull_policy="IfNotPresent"
                , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
                , labels={"foo": "bar"}
                , volumes=[volume]
                , volume_mounts=[volume_mount]
                , is_delete_operator_pod=True
                , in_cluster=True
                )
            split_old >> smp_mod >> split_new
            count+=1

        child_cleanup=KubernetesPodOperator(
            namespace='airflow'
            , name="cleanup_smp-" + mod
            , task_id="cleanup_smp-" + mod
            , image="babadillo12345/airflow-plant:scrape_worker-1.2"
            , cmds=["bash", "-cx"]
            , arguments=["git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/realestate.py -mod cleanup_child"]  
            , image_pull_policy="IfNotPresent"
            , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
            , labels={"foo": "bar"}
            , volumes=[volume]
            , volume_mounts=[volume_mount]
            , is_delete_operator_pod=True
            , in_cluster=True
            )
        split_new >> child_cleanup