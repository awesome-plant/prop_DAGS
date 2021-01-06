# https://www.aylakhan.tech/?p=655
# https://stackoverflow.com/questions/62686753/airflow-dag-id-could-not-be-found-issue-when-using-kubernetes-executor
import logging
import datetime
import os
from airflow import models
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator
# from airflow.contrib.kubernetes.volume_mount import VolumeMount
# from airflow.contrib.kubernetes.volume import Volume
# from airflow.contrib.operators import KubernetesOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago
from airflow import DAG

volume_mount = k8s.V1VolumeMount(
    mount_path='/usr/local/airflow/xmlsave'
    , name='persist-xmlsave'
    , sub_path=None
    , read_only=False
)

volume = k8s.V1Volume(
    name='xmlsave'
    ,persistent_volume_claim=k8s.V1HostPathVolumeSource(path='xmlsave'),
)


init_container_volume_mounts = k8s.V1VolumeMount(
    mount_path='/usr/local/airflow/xmlsave'
    , name='persist-xmlsave'
    , sub_path=None
    , read_only=False
    )

init_container = k8s.V1Container(
    name="kubePod_init-container"
    ,image="alpine/git"
    ,volume_mounts= [init_container_volume_mounts]
    ,command=["bash", "-cx"]
    ,args=["echo test-kube-print-output"]
    )

default_args = {
    'owner': 'airflow',
}


with DAG(
    dag_id='example_kubernetes_operator',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    k = KubernetesPodOperator(
    namespace='airflow'
        , name="airflow-test-pod"
        , task_id="task"
        , image="python:rc-slim"
        , cmds=["python","-c"]
        , arguments=["import time; print('hello world'); time.sleep(600); print('done')"]        
        , labels={"foo": "bar"}
        # , secrets=[secret_file, secret_env, secret_all_keys]
        # , ports=[port]
        , volumes=[volume]
        , volume_mounts=[volume_mount]
        # , env_from=configmaps
        # , affinity=affinity
        , is_delete_operator_pod=True
        , hostnetwork=False
        # , tolerations=tolerations
        , init_containers=[init_container]
        , priority_class_name="medium"
    )









# args = { 'owner': 'airflow' }
# YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)
# # volume_mount = k8s.V1VolumeMount(
# #     name='xmlsave',
# #     mount_path='/usr/local/airflow/xmlsave',
# #     sub_path=None,
# #     read_only=False
# #     )
# volume = k8s.V1Volume(
#     name='xmlsave'
#     ,persistent_volume_claim=k8s.V1HostPathVolumeSource(path='xmlsave'),
# )
# volumemount = k8s.V1VolumeMount(
#     mount_path='/usr/local/airflow/xmlsave'
#     , name='persist-xmlsave'
#     , sub_path=None
#     , read_only=False
# )

# #now we pull in the scripts repo 
# git_repo='https://github.com/awesome-plant/prop_DAGS.git'
# git_branch='NonProd_DAG'
# git_saveDir='/usr/local/airflow/'
# git_command = 'git clone -depth=1 -branch ' + git_branch + ' ' + git_repo + ' cd ' + git_saveDir


# init_container = k8s.V1Container(
#     name="kubePod_init-container"
#     ,image="alpine/git"
#     ,command=["bash", "-cx"]
#     ,args=["echo test-kube-print-output"]
#     ,volume_mounts= [volumemount]
#     )

# try: 
#     print("Entered try block")
#     with models.DAG(
#             dag_id='sampledag',
#             schedule_interval=datetime.timedelta(days=1),
#             start_date=YESTERDAY) as dag:
#                 print("Initialized dag")
#                 kubernetes_min_pod = KubernetesPodOperator(
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
                    ,arguments=["import time; print('hello world'); time.sleep(600); print('done')"]
                    # ,init_containers=[init_container]
                    ,volumes=[volume]
                    ,volume_mounts= [volumemount]
                    # ,affinty=affinity 
                    ,is_delete_operator_pod=False
                    # ,dag=dag
                    )
#                 print("done")

# except Exception as e:
#     print(str(e))
#     logging.error("Error at {}, error={}".format(__file__, str(e)))
#     raise