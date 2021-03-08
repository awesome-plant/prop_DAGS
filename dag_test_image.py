#actually scrapes pages for data from website
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import mods.db_import as db_import
import mods.proxy as proxy
import mods.scrape_site as scrape_site
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

with DAG(
    dag_id='dag_scrape_pages',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    split_old = DummyOperator(task_id='dummy_starter',trigger_rule='all_done')
    split_new = DummyOperator(task_id='dummy_ender',trigger_rule='all_done' )
    image_test = KubernetesPodOperator(
            namespace='airflow'
            , name="test_image"
            , task_id="test_image
            , image="babadillo12345/airflow-plant:scrape_worker-1.2"
            , cmds=["bash", "-cx"]
            , arguments=["git clone https://github.com/awesome-plant/prop_DAGS.git && python prop_DAGS/mods/tets_image.py -mod wait_time -sl 1000"]  
            , image_pull_policy="IfNotPresent"
            , resources={'limit_cpu' : '50m','limit_memory' : '512Mi'}  
            , labels={"foo": "bar"}
            , volumes=[volume]
            , volume_mounts=[volume_mount]
            , is_delete_operator_pod=True
            , in_cluster=True
            )
    split_old >> image_test >> split_new