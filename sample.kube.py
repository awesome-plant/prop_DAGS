# https://stackoverflow.com/questions/62686753/airflow-dag-id-could-not-be-found-issue-when-using-kubernetes-executor
import logging
import datetime

from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator
# from airflow.contrib.operators import KubernetesOperator
import os
from kubernetes.client import models as k8s

args = { 'owner': 'airflow' }

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

volume_mount = [
                k8s.V1VolumeMount(
                    name='xmlsave'
                    ,mount_path='/usr/local/airflow/xmlSave'
                    ,sub_path=None
                    ,read_only=True
                    )
                ]

# init_container_volume_mounts = [k8s.V1VolumeMount(name='xmlsave',
#                                 mount_path='/usr/local/airflow/xmlSave',
#                                 sub_path=None,
#                                 read_only=False)
#     # init_container_volume_mounts = [
#     # k8s.V1VolumeMount(mount_path='/etc/foo', name='test-volume', sub_path=None, read_only=True)
# ]   

try:
    print("Entered try block")
    with models.DAG(
            dag_id='sampledag',
            schedule_interval=datetime.timedelta(days=1),
            start_date=YESTERDAY) as dag:
                print("Initialized dag")
                kubernetes_min_pod = kubernetes_pod_operator.KubernetesPodOperator(
                    # The ID specified for the task.
                    task_id='trigger-task'
                    # Name of task you want to run, used to generate Pod ID.
                    ,name='trigger-name'
                    ,namespace='airflow'
                    ,in_cluster=True
                    ,image="python:rc-slim"
                    ,image_pull_policy='IfNotPresent'
                    ,resources={'limit_cpu' : '500m','limit_memory' : '1024Mi'}
                    ,labels={"foo": "bar"}
                    ,get_logs=True
                    ,cmds=["python","-c"]
                    ,arguments=["import time; print('hello world'); time.sleep(200); print('done')"]
                    ,volume_mounts=volume_mount
                    # ,cmds=["./docker-run.sh"]
                    ,is_delete_operator_pod=False
                    ,dag=dag)
                print("done")

except Exception as e:
    print(str(e))
    logging.error("Error at {}, error={}".format(__file__, str(e)))
    raise