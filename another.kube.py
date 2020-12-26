# https://airflow.apache.org/docs/apache-airflow/stable/_modules/airflow/example_dags/example_kubernetes_executor_config.html
import logging
import os
import time

from airflow import DAG
# from airflow.example_dags.libs.helper import print_stuff
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.kubernetes_operator import KubernetesExecutor
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

default_args = {
    'owner': 'airflow',
}

log = logging.getLogger(__name__)

volume = k8s.V1Volume(
            name='xmlsave', persistent_volume_claim=k8s.V1HostPathVolumeSource(path='xmlsave'),
        )
volumemount = k8s.V1VolumeMount(
                mount_path='/usr/local/airflow/xmlsave', name='persist-xmlsave', sub_path=None, read_only=False
            )


def print_this():
    print('start wait!')
    time.sleep(600)
    print('end wait!')


with DAG(
    dag_id='example_kubernetes_executor_config',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example3'],
) as dag:

    start_task = BashOperator(
        task_id='start_task', bash_command='echo starting_scrape_process', executor_config={
            'pod_override': k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={'test': 'annotation'}))
        },
    )
    # https://www.aylakhan.tech/?p=655
    example_task = PythonOperator(
     task_id='exmaple_task' 
    , python_callable=print_this
    , executor_config={
        'KubernetesExecutor': {
                                'request_cpu': '1'
                                , 'request_memory': '128Mi'
                                , 'limit_memory': '128Mi'
                                ,'volumes': [ {
                                    'name': 'xmlsave'
                                    ,'persistentVolumeClaim':{'claimName': 'xmlsave' }
                                    }
                                ]
                                ,'volume_mounts': [ {
                                    'name': 'xmlsave',
                                    'mountPath': '/usr/local/airflow/xmlsave'
                                    }
                                ]
                            }
                        }
    )

    start_task >> example_task
