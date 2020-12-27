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
#scrape libs libs 
import logging
import os
import datetime 
import time
import requests
import urllib.request
from lxml import etree
from fake_useragent import UserAgent
# import lxml.etree as ET
import pandas as pd 
#airflow libs
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s

default_args={
    'owner': 'Airflow'
    ,'start_date': datetime.datetime.now() - datetime.timedelta(days=1) #yesterday
    }

def ScrapeURL(baseurl,PageSaveFolder, **kwargs):  
    XMLsaveFile="XML_scrape_" + (datetime.datetime.now()).strftime('%Y-%m-%d')
    ua = UserAgent()
    headers = {'User-Agent':str(ua.random)}
    # headers = { 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36', }
    response = requests.get(baseurl,headers=headers)
    xmlFile=PageSaveFolder + '\\' + XMLsaveFile
    # time.sleep(600)
    saveXML=open(xmlFile +'.xml', "w")
    saveXML.write(response.text)
    saveXML.close()
    print("file saved to: " + xmlFile +'.xml')
    #parse to XML 
    result = response.content 
    root = etree.fromstring(result) 
    #scrape variables
    _Suffix=_Filename=_LastModified=_Size=_StorageClass=_Type=""
    XMLDataset=pd.DataFrame(columns =['ScrapeDT','Suffix', 'FileName', 'FileType', 'LastMod', 'Size', 'StorageClass'])
    #iteration is done literally one aspect at a time, since xml wouldnt play nice
    #print element.tag to understand
    for element in root.iter():
        if str(element.tag).replace("{http://s3.amazonaws.com/doc/2006-03-01/}","") == 'Contents' and _Filename != '':
            #write to pd 
            XMLDataset=XMLDataset.append({
                    'ScrapeDT' : (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
                    , 'Suffix' : str(_Suffix)
                    , 'FileName' : str(_Filename)
                    , 'FileType' : str(_Type)
                    , 'LastMod': str(_LastModified)
                    , 'Size' : str(_Size)
                    , 'StorageClass': str(_StorageClass)
                    } ,ignore_index=True) 
            _Suffix=_Filename=_LastModified=_Size=_StorageClass=_Type=""
        elif str(element.tag).replace("{http://s3.amazonaws.com/doc/2006-03-01/}","") == 'Key':
            _Filename=str(element.text)
            _Suffix=str(element.text).split('-')[0]
            #get name subcat
            if 'buy' in _Filename.lower(): 
                _Type='buy'
            elif 'sold' in _Filename.lower(): 
                _Type='sold' 
            elif 'rent' in _Filename.lower(): 
                _Type='rent' 
            else: _Type=''
        elif str(element.tag).replace("{http://s3.amazonaws.com/doc/2006-03-01/}","") == 'LastModified':
            _LastModified=str(element.text)
        elif str(element.tag).replace("{http://s3.amazonaws.com/doc/2006-03-01/}","") == 'Size':
            _Size=str(element.text)
        elif str(element.tag).replace("{http://s3.amazonaws.com/doc/2006-03-01/}","") == 'StorageClass':
            _StorageClass=str(element.text)
    XMLDataset.to_csv(xmlFile +'.csv')
    print("file saved to: " + xmlFile +'.csv')
    #used in next part
    # return XMLDataset

   
dag = DAG(
        dag_id='use_getXML_Scrape'
        ,default_args=default_args
        ,schedule_interval=None
        ,start_date=days_ago(1)
        ,tags=['get_xml_scrape']
    )
starter = DummyOperator( dag = dag, task_id='dummy_starter' )
scrape_task = PythonOperator(
    task_id="scrape_sitemap_rawxml"
    ,provide_context=True
    ,op_kwargs={
        'baseurl':'https://www.realestate.com.au/xml-sitemap/'
        # , 'RootDir': '/opt/airflow/logs/XML_save_folder' 
        , 'PageSaveFolder' : '/opt/airflow/logs/XML_save_folder'
        }
    ,python_callable=ScrapeURL
    )

# https://stackoverflow.com/questions/52558018/airflow-generate-dynamic-tasks-in-single-dag-task-n1-is-dependent-on-taskn
XMLDataset= pd.read_csv('/opt/airflow/logs/XML_save_folder/XML_scrape_' + (datetime.datetime.now()).strftime('%Y-%m-%d') +'.csv')
# a[]
for i in range(0,XMLDataset.shape[0]):
    xml_gz=BashOperator(
            task_id='scrape_sitemap_gz_'+str(i),
            bash_command='echo _' + str(i) + '_' + XMLDataset['FileName'].loc[i] ,
            xcom_push=True,
            dag=dag
            )
        
    
    starter >> scrape_task  >> xml_gz #a[i]
# starter >> scrape_task 