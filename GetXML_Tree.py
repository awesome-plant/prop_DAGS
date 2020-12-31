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
import pandas as pd 
import gzip
import shutil 
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
    XMLDataset=pd.DataFrame(columns =['ScrapeDT','Suffix', 'FileName', 'FileType', 'LastMod', 'Size', 'StorageClass', 'ExternalIP'])

    #get external IP https://stackoverflow.com/questions/2311510/getting-a-machines-external-ip-address-with-python
    _external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')

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
                    , 'ExternalIP': str(_external_ip)
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

def SaveScrape(baseurl, PageSaveFolder, ScrapeFile, **kwargs):
    #download from sitemap, use dynamic variable 
    sitemap_url = baseurl #'https://www.realestate.com.au/xml-sitemap/'#pdp-sitemap-buy-1.xml.gz' 
    _file=ScrapeFile #im lazy, sue me
    gz_save_name =_file[:-7] + '_' + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.gz'
    gz_url = sitemap_url + _file
    gz_save_path = PageSaveFolder

    urllib.request.urlretrieve(sitemap_url + _file, gz_save_path + gz_save_name)
    #save gz to dir for archiving 
    print("file:", gz_save_name)
    print("written to dir:", gz_save_path + gz_save_name)
    #feast upon that rich gooey xml 
    _xml_save = _file[:-7] + '_' + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.xml'  
    with gzip.open(gz_save_path + gz_save_name, 'rb') as f_in:
        with open(gz_save_path + _xml_save, 'wb') as f_out: 
            shutil.copyfileobj(f_in, f_out)
    #xml part 
    root = etree.parse(gz_save_path + _xml_save)
    XML_gz_Dataset=pd.DataFrame(columns =['Parent_gz','ScrapeDT','Url', 'PropType', 'State', 'Suburb', 'PropID', 'LastMod', 'ExternalIP'])
    _PropType=_State=_PropID=_LastMod=_split=_Url=""
    _external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')
    #iterate xml
    for element in root.iter():
        #writes results to df, same as the previous module 
        if 'url' in element.tag and _Url != '':
            XML_gz_Dataset=XML_gz_Dataset.append({
                        'Parent_gz': str(ScrapeFile)
                        ,'ScrapeDT' : (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
                        , 'Url' : str(_Url)
                        , 'PropType' : str(_PropType)
                        , 'State' : str(_State)
                        , 'Suburb' : str(_Suburb)
                        , 'PropID' : str(_PropID)
                        , 'LastMod': str(_LastMod)
                        , 'ExternalIP': str(_external_ip)
                        } ,ignore_index=True) 
            _PropType=_State=_PropID=_LastMod=_split=_Url=""
        if 'lastmod' in element.tag: 
            _LastMod = element.text
        #just about everything gleaned from loac (url) tag
        elif 'loc' in element.tag: 
            if '-tas-' in element.text: 
                _State='tas'
            elif '-vic-' in element.text: 
                _State='vic'
            elif '-nsw-' in element.text: 
                _State='nsw'
            elif '-act-' in element.text: 
                _State='act'
            elif '-qld-' in element.text: 
                _State='qld'
            elif '-nt-' in element.text: 
                _State='nt'
            elif '-sa-' in element.text: 
                _State='sa'
            elif '-wa-' in element.text: 
                _State='wa'
            
            _Url = element.text
            _split=str(element.text).split(_State)
            #had to do it this way so unconventional suburb names are still caught
            _PropType = _split[0].replace('https://www.realestate.com.au/property-','')[:-1]
            _split=str(element.text).split('-')
            _Suburb=_split[len(_split) -2 ]
            _PropID=_split[len(_split) -1 ]

    XML_gz_Dataset.to_csv(gz_save_path + '\\gz_files\\' + _xml_save[:-3] + '_results' +'.csv')
    print("file saved to: " + gz_save_path + '\\gz_files\\' + _xml_save[:-3] + '_results' +'.csv')
   
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
        , 'PageSaveFolder': '/opt/airflow/logs/XML_save_folder'
        }
    ,python_callable=ScrapeURL
    ,dag = dag
    )
starter >> scrape_task 

# https://stackoverflow.com/questions/52558018/airflow-generate-dynamic-tasks-in-single-dag-task-n1-is-dependent-on-taskn
for x in os.scandir('/opt/airflow/logs/XML_save_folder/'):
    if x.name == 'XML_scrape_' + (datetime.datetime.now()).strftime('%Y-%m-%d') +'.csv':
        XMLDataset= pd.read_csv('/opt/airflow/logs/XML_save_folder/XML_scrape_' + (datetime.datetime.now()).strftime('%Y-%m-%d') +'.csv')
        for i in range(0, XMLDataset[XMLDataset['FileType'].notnull()].iloc[0:1].shape[0]):
        # XMLDataset[XMLDataset['FileType'].notnull()].shape[0]): 
            xml_gz_extract=PythonOperator(
                    task_id='scrape_sitemap_gz_'+str(i)
                    ,provide_context=True
                    ,op_kwargs={
                        'baseurl': 'https://www.realestate.com.au/xml-sitemap/'
                        , 'PageSaveFolder': '/opt/airflow/logs/XML_save_folder/gz_files'
                        , 'ScrapeFile': XMLDataset[XMLDataset['FileType'].notnull()]['FileName'].iloc[0:i + 1].to_string(index=False).strip() #pass in filename from filtered iteration
                        }
                    ,python_callable=SaveScrape
                    ,dag=dag
                    )
            starter >> scrape_task  >> xml_gz_extract #a[i]
        # starter >> scrape_task 