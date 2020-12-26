# https://www.aylakhan.tech/?p=655
# https://stackoverflow.com/questions/62686753/airflow-dag-id-could-not-be-found-issue-when-using-kubernetes-executor

# this script does the following: 
# 1. connect to sitemap and parse page as xml 
# 2. for each link on xml 
#     a. give link to kube pod 
#     b. pod does the following: 
#         i. downloads zip 
#         ii. extracts zip 
#         iii. parses contents as xml 
#         iv. converts to csv 
#         v. saves csv 
#kube libs 
import logging
import os
import datetime 
import time
from airflow import DAG
# from airflow.example_dags.libs.helper import print_stuff
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

import requests
import urllib.request
import gzip
import shutil 

default_args={
    'owner': 'Airflow'
    ,'start_date': datetime.datetime.now() - datetime.timedelta(days=1) #yesterday
    }

def ScrapeURL(baseurl,PageSaveXML, **kwargs):  
    XMLsaveFile="XML_scrape_" + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.xml'
    #create browser header for requests 
    # ua = UserAgent()
    #print(ua.chrome)
    # headers = {'User-Agent':str(ua.random)}
    #how many pages are there?
    headers = { 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36', }
    response = requests.get(baseurl,headers=headers)
    # y=BeautifulSoup(response.text, features="html.parser")
    XmFileDir=os.path.join(PageSaveXML, "DL_Files")
    
    xmlFile=os.path.join(XmFileDir, XMLsaveFile)
    time.sleep(600)
    saveXML=open(xmlFile, "w")
    saveXML.write(response.text) #y.prettify())
    saveXML.close()
    print("file saved to: " + xmlFile)

with DAG(
        dag_id='use_getXML_Scrape'
        ,default_args=default_args
        ,schedule_interval=None
        ,start_date=days_ago(1)
        ,tags=['get_xml_scrape']
    ) as dag:    
    
    # You can use annotations on your kubernetes pods!
    start_task = BashOperator(
        task_id="start_task"
        ,bash_command='echo starting_scrape_process'
        ,executor_config={
            "pod_override": k8s.V1Pod(metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"}))
        },
    )

    # [START task_with_volume]
    task_with_volume = PythonOperator(
        task_id="task_with_volume"
        ,provide_context=True
        ,op_kwargs={
            'baseurl':'https://www.realestate.com.au/xml-sitemap/'
            , 'RootDir': '/opt/airflow//xmlsave'
            , 'PageSaveXML' : 'DL_Files/DL_Landing'
            # , 'XMLsaveFile':'XML_scrape_' +
            }
        ,python_callable=ScrapeURL
        ,executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name='persist-xmlsave',
                            volume_mounts=[
                                k8s.V1VolumeMount(
                                    mount_path='/opt/airflow//xmlsave', name='persist-xmlsave'
                                )
                            ],
                        )
                    ],
                    volumes=[
                        k8s.V1Volume(
                            name='persist-xmlsave',
                            host_path=k8s.V1HostPathVolumeSource(path='/D/shared_folder/xmlsave'),
                        )
                    ],
                )
            ),
        },
    )
    # # [END task_with_volume]
    # start_task = DummyOperator(
    #     task_id='DNU_start_task'
    #     # , retries=3
    #     )
    # t1_Get_Sitemap_Tree = PythonOperator(
    #     task_id="DNU_t1_Get_Sitemap_Tree"
    #     ,provide_context=True
    #     ,op_kwargs={
    #         'baseurl':'https://www.realestate.com.au/xml-sitemap/'
    #         , 'RootDir': '/opt/airflow//xmlsave'
    #         , 'PageSaveXML' : 'DL_Files/DL_Landing'
    #         # , 'XMLsaveFile':'XML_scrape_' +
    #         }
    #     ,python_callable=ScrapeURL
    #     # ,dag=dag
    #     )
    start_task >> task_with_volume 
    # start_task >> t1_Get_Sitemap_Tree