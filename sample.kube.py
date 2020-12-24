# https://www.aylakhan.tech/?p=655
# https://stackoverflow.com/questions/62686753/airflow-dag-id-could-not-be-found-issue-when-using-kubernetes-executor
import logging
import datetime
import os
# from airflow import models
# from airflow.contrib.operators import kubernetes_pod_operator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
# from airflow.contrib.kubernetes.volume_mount import VolumeMount
# from airflow.contrib.kubernetes.volume import Volume
# from airflow.contrib.operators import KubernetesOperator
from kubernetes.client import models as k8s

from airflow import DAG

args = { 'owner': 'airflow' }
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
volume_mount = k8s.V1VolumeMount(
                            'xmlsave',
                            mount_path='/usr/local/airflow/xmlsave',
                            sub_path=None,
                            read_only=False
                            )
volume_config = {
    'persistentVolumeClaim':
    {
        'claimName': 'persist-xmlsave'  # uses the persistentVolumeClaim given in the Kube yaml
    }
    }
volume = k8s.V1Volume(
    name='xmlsave',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='xmlsave'),
)
# volume = Volume(
#                 'xmlsave'
#                 , configs=volume_config
#                 ) # the name here is the literal name given to volume for the pods yaml.

#now we pull in the scripts repo 
git_repo='https://github.com/awesome-plant/prop_DAGS.git'
git_branch='NonProd_DAG'
git_saveDir='/usr/local/airflow/'
git_command = 'git clone -depth=1 -branch ' + git_branch + ' ' + git_repo + ' cd ' + git_saveDir


init_container = k8s.V1Container(
    name="kubePod_init-container",
    image="alpine/git",
    # env=init_environments,
    # volume_mounts=init_container_volume_mounts,
    command=["bash", "-cx"],
    args=["echo test-kube-print-output"]
    
)

with DAG(
    dag_id='example_kubernetes_operator',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    k = KubernetesPodOperator(
        namespace='default',
        image="ubuntu:16.04",
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        # secrets=[secret_file, secret_env, secret_all_keys],
        # ports=[port],
        volumes=[volume],
        volume_mounts=[volume_mount],
        # env_from=configmaps,
        name="airflow-test-pod",
        task_id="task",
        # affinity=affinity,
        is_delete_operator_pod=True,
        hostnetwork=False,
        # tolerations=tolerations,
        init_containers=[init_container],
        priority_class_name="medium",
    )

# try:
#     print("Entered try block")
#     with models.DAG(
#             dag_id='sampledag',
#             schedule_interval=datetime.timedelta(days=1),
#             start_date=YESTERDAY) as dag:
#                 print("Initialized dag")
#                 kubernetes_min_pod = kubernetes_pod_operator.KubernetesPodOperator(
#                     task_id='trigger-task'
#                     ,name='trigger-name'
#                     ,namespace='airflow'
#                     # ,in_cluster=True
#                     ,image="python:rc-slim"
#                     ,image_pull_policy='IfNotPresent'
#                     ,resources={'limit_cpu' : '500m','limit_memory' : '512Mi'}
#                     ,labels={"foo": "bar"}
#                     ,get_logs=True
#                     ,cmds=["python","-c"]
#                     ,arguments=["import time; print('hello world'); time.sleep(600); print('done')"]
#                     ,init_containers=[init_container]
#                     ,volumes=[
#                         Volume("persist-xmlsave",
#                             {
#                                 "persistentVolumeClaim":
#                                 {
#                                     "claimName": "persist-xmlsave"
#                                 }
#                             })
#                         ]
#                     ,volume_mounts=[ 
#                         VolumeMount("persist-xmlsave", "/usr/local/airflow/xmlsave", sub_path=None, read_only=False)
#                         ]
#                     # ,affinty=affinity 
#                     ,is_delete_operator_pod=False
#                     ,dag=dag)
#                 print("done")

# except Exception as e:
#     print(str(e))
#     logging.error("Error at {}, error={}".format(__file__, str(e)))
#     raise