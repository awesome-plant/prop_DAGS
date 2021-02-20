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
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
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
#iterate to run 
l_proxy_mods=["scrape_parent"]

with DAG(
    dag_id='dag_get_sitemap_parent',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    sitemap_starter = DummyOperator(task_id='dummy_starter' )
    sitemap_ender = DummyOperator(task_id='dummy_ender' )
    for mod in l_proxy_mods:
        smp_mod = KubernetesPodOperator(
            namespace='airflow'
            , name="get_smp-" + mod
            , task_id="get_smp-" + mod
            , image="babadillo12345/airflow-plant:scrape_worker-1.1"
            , cmds=["bash", "-cx"]
            , arguments=["git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/sitemap.py -mod " + mod]  
            , image_pull_policy="IfNotPresent"
            , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
            , labels={"foo": "bar"}
            , volumes=[volume]
            , volume_mounts=[volume_mount]
            , is_delete_operator_pod=True
            , in_cluster=True
            )
        sitemap_starter >> smp_mod >> sitemap_ender 