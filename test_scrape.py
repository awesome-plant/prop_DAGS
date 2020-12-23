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
import datetime
from datetime import datetime, timedelta
import os
from airflow import models
from airflow.contrib.operators import kubernetes_pod_operator
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.volume import Volume
from kubernetes.client import models as k8s
from airflow import DAG
#regular scrape libs 
from airflow.operators.python_operator import PythonOperator 

#check/install packages 
import pip 
# package = 'package_name'
try:
    __import__('requests')
except ImportError:
    # pip(['install', 'requests'])  
     os.system("pip install requests")#+ package)
try:
    __import__('fake_useragent')
except ImportError:
    os.system("pip install fake_useragent")

import requests
import urllib.request
from fake_useragent import UserAgent 
     #scrape formatting
from bs4 import BeautifulSoup
import pandas as pd
from pandas import DataFrame
    #data saving
import os
#extract from gt file 
import gzip
import shutil 
try:
    from StringIO import StringIO
except:
    from io import StringIO

def ScrapeURL(baseurl, RootDir, PageSaveXML, PageSaveCSV, **kwargs):  
    XMLsaveFile="XML_sitemap_" + (datetime.now()).strftime('%Y-%m-%d') + '.xml'
    #create browser header for requests 
    ua = UserAgent()
    #print(ua.chrome)
    headers = {'User-Agent':str(ua.random)}
    #how many pages are there?
    #headers = { 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36', }
    response = requests.get(baseurl,headers=headers)
    y=BeautifulSoup(response.text, features="html.parser")
    #save xml to dir, will be read again later 
    # XMLFile=os.path.join(RootDir + "\\DL_Files\\", file.strip(' \t\n\r') )
    XmFileDir=os.path.join(RootDir, PageSaveXML)
    print(str(XmFileDir))
    try: 
        os.makedirs(XmFileDir)
        print("made dir: " + XmFileDir)
    except Exception as e: 
        # pass
        print("couldnt make dir: " + XmFileDir) 
        print(e)    
    xmlFile=os.path.join(XmFileDir, XMLsaveFile)
    saveXML=open(xmlFile, "w")
    saveXML.write(y.prettify())
    saveXML.close()
    print("file saved to: " + xmlFile)
  
args={
    'owner': 'Airflow'
    # ,'start_date': airflow.utils.dates.days_ago(1)   
    ,'retries': 1
    ,'retry_delay': timedelta(minutes=1)
    ,'schedule_interval': '@daily'
    ,'start_date': datetime(2020, 3, 25)

}

dag = DAG(
    dag_id='GETXML_TO_CSV'
    ,catchup=False
    ,default_args=args
)

t1_Get_Sitemap_Tree = PythonOperator(
    task_id="t1_Get_Sitemap_Tree"
    ,provide_context=True
    ,op_kwargs={
          'baseurl':'https://www.realestate.com.au/xml-sitemap/'
        , 'RootDir': '/usr/local/airflow/xmlsave'
        , 'PageSaveXML' : 'DL_Files/DL_Landing'
        # , 'XMLsaveFile':'XML_scrape_' +
        }
    ,python_callable=ScrapeURL
    ,dag=dag
)


t1_Get_Sitemap_Tree