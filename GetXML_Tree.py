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
#postgres db stuff 
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import Error
import csv
from io import StringIO
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

    #get external IP https://stackoverflow.com/questions/2311510/getting-a-machines-external-ip-address-with-python
    H_external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')
    H_ScrapeDT=(datetime.datetime.now())
    # headers = { 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36', }
    response = requests.get(baseurl,headers=headers)
    xmlFile=PageSaveFolder + XMLsaveFile 

    # time.sleep(600)
    saveXML=open(xmlFile +'.xml', "w")
    saveXML.write(response.text)
    saveXML.close()
    print("temp file saved to: " + xmlFile +'.xml')
    H_FileSize=round(os.path.getsize(xmlFile +'.xml') / 1000)
    #write to parent table, apparently im storing in 3rd normal
    XML_H_Dataset=pd.DataFrame(columns =['external_ip', 'folderpath', 'h_filename', 'scrape_dt', 'h_filesize_kb','h_fileid'])
    # XML_H_Dataset.dtypes
    XML_H_Dataset=XML_H_Dataset.append({
                    'external_ip': str(H_external_ip)
                    , 'folderpath': str(PageSaveFolder)
                    , 'h_filename': str(XMLsaveFile + '.xml')
                    , 'scrape_dt': H_ScrapeDT
                    , 'h_filesize_kb': int(H_FileSize)
                    } ,ignore_index=True) 
    XML_H_Dataset['h_fileid']=pd.to_numeric(XML_H_Dataset['h_fileid'])
    # XML_H_Dataset.to_datetime()

    #parse to XML 
    result = response.content 
    root = etree.fromstring(result) 
    #scrape variables
    _Suffix=_Filename=_LastModified=_Size=_StorageClass=_Type=""
    XML_S_Dataset=pd.DataFrame(columns =['suffix', 's_filename', 'filetype', 'lastmod', 's_filesize_kb', 'storageclass', 'h_fileid'])

    #iteration is done literally one aspect at a time, since xml wouldnt play nice
    #print element.tag to understand
    for element in root.iter():
        if str(element.tag).replace("{http://s3.amazonaws.com/doc/2006-03-01/}","") == 'Contents' and _Filename != '':
            #write to pd 
            XML_S_Dataset=XML_S_Dataset.append({
                    'suffix' : str(_Suffix)
                    , 's_filename' : str(_Filename)
                    , 'filetype' : str(_Type)
                    , 'lastmod': _LastModified
                    , 's_filesize_kb' : round(int(_Size) / 1000)
                    , 'storageclass': str(_StorageClass)
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
            _LastModified=datetime.datetime.strptime((element.text).replace('Z','+0000'), '%Y-%m-%dT%H:%M:%S.%f%z')
        elif str(element.tag).replace("{http://s3.amazonaws.com/doc/2006-03-01/}","") == 'Size':
            _Size=str(element.text)
        elif str(element.tag).replace("{http://s3.amazonaws.com/doc/2006-03-01/}","") == 'StorageClass':
            _StorageClass=str(element.text)
    #hard code dtypes for sql later        
    XML_S_Dataset['lastmod']=pd.to_datetime(XML_S_Dataset['lastmod'])
    XML_S_Dataset['s_filesize_kb']=pd.to_numeric(XML_S_Dataset['s_filesize_kb'])
    XML_S_Dataset['h_fileid']=pd.to_numeric(XML_S_Dataset['h_fileid'])
    XML_S_Dataset.to_csv(xmlFile +'.csv')
    print("file saved to: " + xmlFile +'.csv')
    try:
    # Connect to an existing database
        connection = psycopg2.connect(user="postgres",password="root",host="172.22.114.65",port="5432",database="scrape_db")
        cursor = connection.cursor()
        cursor.execute("SELECT coalesce(max(H_FILEID), 0) + 1 as h_fileid from sc_land.SC_SOURCE_HEADER")
        h_fileid = cursor.fetchone() #next iteration of file ID 
        print('new h_fileid is:', h_fileid[0])
    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    print("inserting into tables: sc_source_header, sc_source_file")
    engine = create_engine('postgresql://postgres:root@172.22.114.65:5432/scrape_db')
    XML_H_Dataset.to_sql(
        name='sc_source_header'
        ,schema='sc_land'
        ,con=engine
        ,method=psql_insert_copy
        ,if_exists='append'
        ,index=False
        )

    XML_S_Dataset.to_sql(
        name='sc_source_file'
        ,schema='sc_land'
        ,con=engine
        ,method=psql_insert_copy
        ,if_exists='append'
        ,index=False
    )
    print("inserts completed")
    print('removing extracted xml file')
    os.remove(xmlFile +'.xml')
    print("fin")
    #used in next part
    # return XML_S_Dataset
# https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method
def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)

def SaveScrape(baseurl, PageSaveFolder, ScrapeFile, **kwargs):
    print('gz file:', ScrapeFile)
    #download from sitemap, use dynamic variable 
    sitemap_url = baseurl #'https://www.realestate.com.au/xml-sitemap/'#pdp-sitemap-buy-1.xml.gz' 
    _file=ScrapeFile #im lazy, sue me
    gz_save_name =_file[:-7] + '_' + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.gz'
    gz_url = sitemap_url + _file
    gz_save_path = PageSaveFolder
    urllib.request.urlretrieve(gz_url, gz_save_path + gz_save_name)

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
    XML_gz_Dataset=pd.DataFrame(columns =['parent_gz','scrape_dt','url', 'proptype', 'state', 'suburb', 'prop_id', 'lastmod', 'external_ip', 's_fileid'])
    _PropType=_State=_PropID=_LastMod=_split=_Url=""
    _external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')
    #iterate xml
    for element in root.iter():
        #writes results to df, same as the previous module 
        if 'url' in element.tag and _Url != '':
            XML_gz_Dataset=XML_gz_Dataset.append({
                        'parent_gz': str(ScrapeFile)
                        ,'scrape_dt' : (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
                        , 'url' : str(_Url)
                        , 'proptype' : str(_PropType)
                        , 'state' : str(_State)
                        , 'suburb' : str(_Suburb)
                        , 'prop_id' : str(_PropID)
                        , 'lastmod': str(_LastMod)
                        , 'external_ip': str(_external_ip)
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
    XML_gz_Dataset.to_csv(gz_save_path + '\\parsed_csv\\ ' + _xml_save[:-3] + '_results' +'.csv')
    print("file saved to: " + gz_save_path + '\\parsed_csv\\ ' + _xml_save[:-3] + '_results' +'.csv')
    XML_gz_Dataset['lastmod']=pd.to_datetime(XML_gz_Dataset['lastmod'])
    #now we add to db table 
    #parent file link
    connection = psycopg2.connect(user="postgres",password="root",host="172.22.114.65",port="5432",database="scrape_db")
    cursor = connection.cursor()
    # with connection.cursor() as cursor:
    cursor.execute("""
        select max(s_fileid)
        FROM sc_land.sc_source_file
        WHERE s_filename = %(s_filename)s
        and date(lastmod) = %(lastmod)s;
        """,
            {
                's_filename': XML_gz_Dataset['parent_gz'].drop_duplicates()[0]
                ,'lastmod' : XML_gz_Dataset['lastmod'].dt.date.drop_duplicates()[0]
            }
        )
    result = cursor.fetchone()
    print("parent file link is:",ScrapeFile,"is:", result[0])
    XML_gz_Dataset['s_fileid']=result[0]
    #remove redundant link
    XML_gz_Dataset=XML_gz_Dataset.drop(columns=['parent_gz'])

    #time to insert  
    print("inserting into tables: sc_property_links")
    engine = create_engine('postgresql://postgres:root@172.22.114.65:5432/scrape_db')
    XML_gz_Dataset.to_sql(
        name='sc_property_links'
        ,schema='sc_land'
        ,con=engine
        ,method=psql_insert_copy
        ,if_exists='append'
        ,index=False
        )
    print("insert complete")
    print('removing extracted xml file')
    os.remove(gz_save_path + _xml_save)
    print("fin")
   
dag = DAG(
        dag_id='use_getXML_Scrape'
        ,default_args=default_args
        ,schedule_interval=None
        ,start_date=days_ago(1)
        ,tags=['get_xml_scrape']
    )
starter = DummyOperator( dag = dag, task_id='dummy_starter' )
ender = DummyOperator( dag = dag, task_id='dummy_ender' )
scrape_task = PythonOperator(
    task_id="scrape_sitemap_rawxml"
    ,provide_context=True
    ,op_kwargs={
        'baseurl':'https://www.realestate.com.au/xml-sitemap/'
        # , 'RootDir': '/opt/airflow/logs/XML_save_folder' 
        , 'PageSaveFolder': '/opt/airflow/logs/XML_save_folder/raw_sitemap/'
        }
    ,python_callable=ScrapeURL
    ,dag = dag
    )
starter >> scrape_task 

# https://stackoverflow.com/questions/52558018/airflow-generate-dynamic-tasks-in-single-dag-task-n1-is-dependent-on-taskn
for x in os.scandir('/opt/airflow/logs/XML_save_folder/raw_sitemap'):
    if x.name == 'XML_scrape_' + (datetime.datetime.now()).strftime('%Y-%m-%d') +'.csv':
        XML_H_Dataset= pd.read_csv(x.path) #'/opt/airflow/logs/XML_save_folder/raw_sitemap/XML_scrape_' + (datetime.datetime.now()).strftime('%Y-%m-%d') +'.csv')
        for i in range(0, XML_H_Dataset[XML_H_Dataset['filetype'].notnull()].shape[0]): #.iloc[0:1]
        # XMLDataset[XMLDataset['filetype'].notnull()].shape[0]): 
            xml_gz_extract=PythonOperator(
                    task_id='scrape_sitemap_gz_'+str(i)
                    ,provide_context=True
                    ,op_kwargs={
                        'baseurl': 'https://www.realestate.com.au/xml-sitemap/'
                        , 'PageSaveFolder': '/opt/airflow/logs/XML_save_folder/gz_files/'
                        , 'ScrapeFile': XML_H_Dataset[XML_H_Dataset['filetype'].notnull()]['s_filename'].iloc[0:i + 1].to_string(index=False).strip() #pass in filename from filtered iteration
                        }
                    ,python_callable=SaveScrape
                    ,dag=dag
                    )
            starter >> scrape_task  >> xml_gz_extract >> ender #a[i]
        # starter >> scrape_task 