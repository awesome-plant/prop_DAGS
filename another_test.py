from airflow.contrib.operators import KubernetesOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret

secret_file = Secret('volume', '/etc/sql_conn', 'airflow-secrets', 'sql_alchemy_conn')
secret_env  = Secret('env', 'SQL_CONN', 'airflow-secrets', 'sql_alchemy_conn')
volume_mount = VolumeMount('xmlsave',
                            mount_path='/usr/local/airflow/xmlSave',
                            sub_path=None,
                            read_only=True)

volume_config= {
    'persistentVolumeClaim':
      {
        'claimName': 'xmlsave'
      }
    }
volume = Volume(name='xmlsave', configs=volume_config)

affinity = {
    'nodeAffinity': {
      'preferredDuringSchedulingIgnoredDuringExecution': [
        {
          "weight": 1,
          "preference": {
            "matchExpressions": {
              "key": "disktype",
              "operator": "In",
              "values": ["ssd"]
            }
          }
        }
      ]
    },
    "podAffinity": {
      "requiredDuringSchedulingIgnoredDuringExecution": [
        {
          "labelSelector": {
            "matchExpressions": [
              {
                "key": "security",
                "operator": "In",
                "values": ["S1"]
              }
            ]
          },
          "topologyKey": "failure-domain.beta.kubernetes.io/zone"
        }
      ]
    },
    "podAntiAffinity": {
      "requiredDuringSchedulingIgnoredDuringExecution": [
        {
          "labelSelector": {
            "matchExpressions": [
              {
                "key": "security",
                "operator": "In",
                "values": ["S2"]
              }
            ]
          },
          "topologyKey": "kubernetes.io/hostname"
        }
      ]
    }
}

tolerations = [
    {
        'key': "key",
        'operator': 'Equal',
        'value': 'value'
     }
]

k = KubernetesPodOperator(namespace='airflow',
                          image="python:rc-slim",
                          cmds=["python","-c"],
                          arguments=["import time; print('hello world'); time.sleep(200); print('done')"],
                          labels={"foo": "bar"},
                        #   secrets=[secret_file,secret_env]
                          volume=[volume],
                          volume_mounts=[volume_mount],
                          name="test",
                          task_id="task",
                          affinity=affinity,
                          is_delete_operator_pod=False,
                        #   hostnetwork=False,
                          tolerations=tolerations
                          )