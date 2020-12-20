# https://stackoverflow.com/questions/62686753/airflow-dag-id-could-not-be-found-issue-when-using-kubernetes-executor
import logging
import datetime
import os
from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator
# from airflow.contrib.operators import KubernetesOperator
from kubernetes.client import models as k8s
from airflow import DAG

args = { 'owner': 'airflow' }

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

volume_mount = k8s.V1VolumeMount(
                            name='xmlsave',
                            mount_path='/usr/local/airflow/xmlSave',
                            sub_path=None,
                            read_only=True)

volume = k8s.V1Volume(
    name='xmlsave'
    ,persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name='test-volume'
        ),
    )

affinity = k8s.V1Affinity(
    node_affinity=k8s.V1NodeAffinity(
        preferred_during_scheduling_ignored_during_execution=[
            k8s.V1PreferredSchedulingTerm(
                weight=1,
                preference=k8s.V1NodeSelectorTerm(
                    match_expressions=[
                        k8s.V1NodeSelectorRequirement(key="disktype", operator="in", values=["ssd"])
                    ]
                ),
            )
        ]
    ),
    pod_affinity=k8s.V1PodAffinity(
        required_during_scheduling_ignored_during_execution=[
            k8s.V1WeightedPodAffinityTerm(
                weight=1,
                pod_affinity_term=k8s.V1PodAffinityTerm(
                    label_selector=k8s.V1LabelSelector(
                        match_expressions=[
                            k8s.V1LabelSelectorRequirement(key="security", operator="In", values="S1")
                        ]
                    ),
                    topology_key="failure-domain.beta.kubernetes.io/zone",
                ),
            )
        ]
    ),
)


tolerations = [k8s.V1Toleration(key="key", operator="Equal", value="value")]
  
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
                    ,volumes=[volume]
                    ,volume_mounts=[volume_mount]
                    ,affinity=affinity
                    ,tolerations=tolerations
                    # ,cmds=["./docker-run.sh"]
                    ,is_delete_operator_pod=False
                    ,dag=dag)
                print("done")

except Exception as e:
    print(str(e))
    logging.error("Error at {}, error={}".format(__file__, str(e)))
    raise
