# https://stackoverflow.com/questions/62686753/airflow-dag-id-could-not-be-found-issue-when-using-kubernetes-executor
import logging
import datetime

from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator
import os

args = {
    'owner': 'airflow'
}

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)


try:
    print("Entered try block")
    with models.DAG(
            dag_id='sampledag',
            schedule_interval=datetime.timedelta(days=1),
            start_date=YESTERDAY) as dag:
                print("Initialized dag")
                kubernetes_min_pod = kubernetes_pod_operator.KubernetesPodOperator(
                    # The ID specified for the task.
                    task_id='trigger-task',
                    # Name of task you want to run, used to generate Pod ID.
                    name='trigger-name',
                    namespace='scheduler',
                    in_cluster=True,

                    cmds=["./docker-run.sh"],
                    is_delete_operator_pod=False,
                    image='imagerepo:latest',
                    image_pull_policy='Always',
                    dag=dag)

        print("done")

except Exception as e:
    print(str(e))
    logging.error("Error at {}, error={}".format(__file__, str(e)))
    raise
