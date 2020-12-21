import datetime
import unittest
from unittest import TestCase
from airflow.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount


class TestMailAlarm(TestCase):
    def setUp(self):
        self.namespace = "test-namespace"
        self.image = "ubuntu:16.04"
        self.name = "default"
        self.cluster_context = "default"
        self.dag_id = "test_dag"
        self.task_id = "root_test_dag"
        self.execution_date = datetime.datetime.now()
        self.context = {"dag_id": self.dag_id,
                        "task_id": self.task_id,
                        "execution_date": self.execution_date}
        self.cmds = ["sleep"]
        self.arguments = ["100"]
        self.volume_mount = VolumeMount('xmlsave',
                                        mount_path='/etc/xmlsave',
                                        sub_path=None,
                                        read_only=False)
        volume_config = {
            'persistentVolumeClaim':
                {
                    'claimName': 'xmlsave'
                }
        }
        self.volume = Volume(name='xmlsave', configs=volume_config)
        self.operator = KubernetesPodOperator(
            namespace=self.namespace, image=self.image, name=self.name,
            cmds=self.cmds,
            arguments=self.arguments,
            startup_timeout_seconds=600,
            is_delete_operator_pod=True,
            # the operator could run successfully but the directory /tmp is not mounted to kubernetes operator
            volumes=[self.volume],
            volume_mounts=[self.volume_mount],
            **self.context)

    def test_execute(self):
        self.operator.execute(self.context)