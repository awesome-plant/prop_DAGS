# https://stackoverflow.com/questions/62686753/airflow-dag-id-could-not-be-found-issue-when-using-kubernetes-executor
import logging
import datetime
import os
from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator
# from airflow.contrib.operators import KubernetesOperator
from kubernetes.client import models as k8s
from airflow.kubernetes.volume_mount import VolumeMount
from airflow import DAG

args = { 'owner': 'airflow' }
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
volume_mount = VolumeMount(
                            name='xmlsave',
                            mount_path='/usr/local/airflow/xmlsave',
                            sub_path=None,
                            read_only=False
                            )
# volume = k8s.V1Volume(
#     name='xmlsave'
#     ,persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
#         claim_name='xmlsave'
#         ),
#     )
# configmaps = [k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='config_name'))]

try:
    print("Entered try block")
    with models.DAG(
            dag_id='sampledag',
            schedule_interval=datetime.timedelta(days=1),
            start_date=YESTERDAY) as dag:
                print("Initialized dag")
                kubernetes_min_pod = kubernetes_pod_operator.KubernetesPodOperator(
                    task_id='trigger-task'
                    ,name='trigger-name'
                    ,namespace='airflow'
                    # ,in_cluster=True
                    ,image="python:rc-slim"
                    ,image_pull_policy='IfNotPresent'
                    ,resources={'limit_cpu' : '500m','limit_memory' : '512Mi'}
                    ,labels={"foo": "bar"}
                    ,get_logs=True
                    ,cmds=["python","-c"]
                    ,arguments=["import time; print('hello world'); time.sleep(2); print('done')"]
                    # ,configmaps=configmaps
                    # ,volumes=[volume]
                    ,volume_mounts=[volume_mount]
                    # ,affinity=affinity
                    # ,tolerations=tolerations
                    # ,cmds=["./docker-run.sh"]
                    ,is_delete_operator_pod=False
                    ,dag=dag)
                print("done")

except Exception as e:
    print(str(e))
    logging.error("Error at {}, error={}".format(__file__, str(e)))
    raise
