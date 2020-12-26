# https://airflow.apache.org/docs/apache-airflow/stable/_modules/airflow/example_dags/example_kubernetes_executor_config.html
import logging
import os

from airflow import DAG
# from airflow.example_dags.libs.helper import print_stuff
from airflow.operators.python_operator import PythonOperator
from airflow.operators.kubernetes_operator import KubernetesExecutor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

log = logging.getLogger(__name__)

volume = k8s.V1Volume(
            name='xmlsave'
            ,persistent_volume_claim=k8s.V1HostPathVolumeSource(path='xmlsave'),
        )
volumemount = k8s.V1VolumeMount(
                mount_path='/usr/local/airflow/xmlsave'
                , name='persist-xmlsave'
                , sub_path=None
                , read_only=False
            )

try:
    from kubernetes.client import models as k8s

    with DAG(
        dag_id='example_kubernetes_executor_config',
        default_args=default_args,
        schedule_interval=None,
        start_date=days_ago(2),
        tags=['example3'],
    ) as dag:

        exmaple_task = PythonOperator(
        task_id='exmaple_task',
        python_callable=print_stuff,
        executor_config={
            'KubernetesExecutor': { 'request_cpu': '1'
                                    ,'request_memory': '128Mi'
                                    ,'limit_memory': '128Mi'
                                    ,'volumes': [volume]
                                    ,'volume_mounts': [volumemount]
                                    }
                        }
        )

        start_task >> volume_task >> third_task
        start_task >> other_ns_task
        start_task >> sidecar_task
        start_task >> task_with_template
except ImportError as e:
    log.warning('Could not import DAGs in example_kubernetes_executor_config.py: %s', str(e))
    log.warning('Install kubernetes dependencies with: pip install apache-airflow['cncf.kubernetes']')