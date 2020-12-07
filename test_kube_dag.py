from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kubernetes_sample', default_args=default_args, schedule_interval=timedelta(minutes=10))


start = DummyOperator(task_id='run_this_first', dag=dag)

passing = KubernetesPodOperator(
                           namespace="airflow"
                          ,name="passing-test"
                          ,task_id="passing-task"
                          ,image="python:rc-slim"
                          ,cmds=["python","-c"]
                          ,resources={'limit_cpu' : '500m','limit_memory' : '1024Mi'}
                          ,arguments=["print('hello world')"]
                          ,image_pull_policy='IfNotPresent'
                          ,labels={"foo": "bar"}
                          ,get_logs=True
                          ,dag=dag
                          )

failing = KubernetesPodOperator(
                           namespace="airflow"
                          ,name="failing-test"
                          ,task_id="failing-task"
                          ,image="python:rc-slim"
                          ,cmds=["python","-c"]
                          ,resources={'limit_cpu' : '500m','limit_memory' : '1024Mi'}
                          ,arguments=["print('hello world')"]
                          ,image_pull_policy='IfNotPresent'
                          ,labels={"foo": "bar"}
                          ,get_logs=True
                          ,dag=dag
                          )

passing.set_upstream(start)
failing.set_upstream(start)
