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
from lxml import etree
# from fake_useragent import UserAgent
from fp.fp import FreeProxy
import pandas as pd 
import gzip
import shutil 
import random
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
# from kubernetes.client import models as k8s

default_args={
    'owner': 'Airflow'
    ,'start_date': datetime.datetime.now() - datetime.timedelta(days=1) #yesterday
    }

def getProxy(): 
    # def here returns proxy, confirmed with different whatismyip return 
    url='https://ident.me/'
    # ua = UserAgent()
    q=requests.get(url)
    _actualIP=q.text
    _newIP=''
    _getIP_time=time.process_time()
    _try=0
    _checkout=False
    time.sleep(random.randint(1,10))
    while _checkout==False: #_newIP != _actualIP :
        #headers = {'User-Agent':str(ua.random)}
        headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
        proxy = FreeProxy(rand=True).get()
        proxies= { 'http': proxy, 'https': proxy } 
        try:
            r = requests.get(url, headers=headers, proxies=proxies)
            _newIP = r.text
            print("realIP is: ", _actualIP, " - proxy IP is:", _newIP, " - attempt no.", str(_try))
        except Exception as e: 
            print('proxy error, sleep 1-10 try again:', e)
            time.sleep(random.randint(1,10))
        if _actualIP !=_newIP:
            _checkout=True
        _try+=1
    print("fin, total time", str( time.process_time() - _getIP_time ) )
    print("realIP is: ", _actualIP, " - proxy IP is:", _newIP, " - attempt no.", str(_try))
    return proxies, _newIP

def ScrapeURL(baseurl, PageSaveFolder, Scrapewait, **kwargs):  
    XMLsaveFile="XML_scrape_" + (datetime.datetime.now()).strftime('%Y-%m-%d')
    time.sleep(random.randint(1,10))
    # ua = UserAgent()
    #headers = {'User-Agent':str(ua.random)}
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}

    proxies,H_external_ip=getProxy()
    # q = request.get('https://ident.me')
    # H_external_ip=q.text
    H_ScrapeDT=(datetime.datetime.now())
    _getPass=False
    #loop until it works, if it takes too long get another proxy 
    while _getPass==False:
        try:
            response = requests.get(baseurl,headers=headers,proxies=proxies, timeout=Scrapewait)
            _getPass=True
        except Exception as e:
            print('error recieved, trying again:',e) 
            proxies,H_external_ip=getProxy()

    xmlFile=PageSaveFolder + XMLsaveFile 
    saveXML=open(xmlFile +'.xml', "w")
    saveXML.close()
    tree = etree.fromstring(response.content ) 
    with open(xmlFile +'.xml', "wb") as saveXML:
        saveXML.write(etree.tostring(tree,pretty_print=True))
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
    #parse to XML 
    result = response.content 
    #scrape variables
    _Suffix=_Filename=_LastModified=_Size=_StorageClass=_Type=""
    XML_S_Dataset=pd.DataFrame(columns =['suffix', 's_filename', 'filetype', 'lastmod', 's_filesize_kb', 'storageclass', 'h_fileid'])
    #FINALLY got this bit working, as always the issue was the namespace
    body=tree.xpath('//ns:Contents',namespaces={'ns':"http://s3.amazonaws.com/doc/2006-03-01/"})
    for element in body:
        _Type=''
        if 'buy' in (element[0].text).lower(): 
            _Type='buy'
        elif 'sold' in (element[0].text).lower(): 
            _Type='sold'
        elif 'rent' in (element[0].text).lower(): 
            _Type='rent'
        XML_S_Dataset=XML_S_Dataset.append({
            'suffix' : str(str(element[0].text).split('-')[0])
            , 's_filename' : str(element[0].text)
            , 'filetype' : str(_Type)
            , 'lastmod': datetime.datetime.strptime((element[1].text).replace('Z','+0000'), '%Y-%m-%dT%H:%M:%S.%f%z')
            , 's_filesize_kb' : round(int(element[3].text) / 1000)
            , 'storageclass': str(element[4].text)
            } ,ignore_index=True) 
    #hard code dtypes for sql later        
    XML_S_Dataset['lastmod']=pd.to_datetime(XML_S_Dataset['lastmod'])
    XML_S_Dataset['s_filesize_kb']=pd.to_numeric(XML_S_Dataset['s_filesize_kb'])
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
    XML_S_Dataset['h_fileid']=h_fileid[0]
    XML_H_Dataset['h_fileid']=h_fileid[0]
    XML_S_Dataset['h_fileid']=pd.to_numeric(XML_S_Dataset['h_fileid'])
    XML_H_Dataset['h_fileid']=pd.to_numeric(XML_H_Dataset['h_fileid'])
    XML_S_Dataset.to_csv(xmlFile +'.csv')
    print("file saved to: " + xmlFile +'.csv')
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

def SaveScrape(baseurl, PageSaveFolder, ScrapeFile, Scrapewait, **kwargs):
    print('gz file:', ScrapeFile)
    time.sleep(random.randint(1,10))
    # ua = UserAgent()
    #headers = {'User-Agent':str(ua.random)}
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    proxies,external_ip=getProxy()

    _getPass=False
        #loop until it works, if it takes too long get another proxy 
    while _getPass==False:
        try:
            response = requests.get(baseurl + ScrapeFile, headers=headers,proxies=proxies, timeout=Scrapewait)
            _getPass=True
        except Exception as e:
            print('error recieved, trying again:',e) 
            proxies,external_ip=getProxy()

    print('gz file:', ScrapeFile)
    #download from sitemap, use dynamic variable 
    sitemap_url = baseurl #'https://www.realestate.com.au/xml-sitemap/'#pdp-sitemap-buy-1.xml.gz' 
    _file=ScrapeFile #im lazy, sue me
    gz_save_name =_file[:-7] + '_' + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.gz'
    gz_url = sitemap_url + _file
    gz_save_path = PageSaveFolder
    #save to gz
    open(gz_save_path + gz_save_name, 'wb').write(response.content)

    #save gz to dir for archiving 
    print("file:", gz_save_name)
    print("written to dir:", gz_save_path + gz_save_name)
    #feast upon that rich gooey xml 
    _xml_save = _file[:-7] + '_' + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.xml'  
    with gzip.open(gz_save_path + gz_save_name, 'rb') as f_in:
        with open(gz_save_path + _xml_save, 'wb') as f_out: 
            shutil.copyfileobj(f_in, f_out)
    tree = etree.parse(gz_save_path + _xml_save)
    with open(gz_save_path + _xml_save, "wb") as saveXML:
        saveXML.write(etree.tostring(tree,pretty_print=True))

    _count=1
    _time=time.time()
    #iterate xml
    body=tree.xpath('//ns:url',namespaces={'ns':"http://www.sitemaps.org/schemas/sitemap/0.9"})
    _count=1
    _time=time.time()
    # XML_gz_Dataset=pd.DataFrame(columns =['parent_gz','scrape_dt','url', 'proptype', 'state', 'suburb', 'prop_id', 'lastmod', 'external_ip', 's_fileid'])
    new_Dataset=   pd.DataFrame(columns =['parent_gz','scrape_dt','url', 'lastmod', 's_fileid'])
    for element in body:
        if _count % 10000 == 0: 
            print("interval:", str(_count-1)," -total runtime:", time.time()-_time)
        # _LastMod = element[1].text
        # _Url = element[0].text
        # #writes results to df, same as the previous module 
        new_Dataset=new_Dataset.append({
                    'parent_gz': str(ScrapeFile)
                    ,'scrape_dt' : (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
                    , 'url' : str(element[0].text)
                    , 'lastmod': str(element[1].text)
                    } ,ignore_index=True) 
        _count+=1 
    print("xml extract time:", time.time() - _time)
    XML_gz_Dataset=new_Dataset

    #add state
    XML_gz_Dataset['state']=XML_gz_Dataset.apply(lambda x: 
        'nsw' if '-nsw-' in x.url else
        'qld' if '-qld-' in x.url else
        'tas' if '-tas-' in x.url else
        'act' if '-act-' in x.url else
        'sa' if '-sa-' in x.url else
        'nt' if '-nt-' in x.url else
        'wa' if '-wa-' in x.url else
        'vic' if '-vic-' in x.url else ''
        , axis=1)

    # get proptype 
    XML_gz_Dataset['proptype']=XML_gz_Dataset['url'].apply(lambda x: 
        x.split('-nsw-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-nsw-')) > 1 else 
        x.split('-qld-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-qld-')) > 1 else 
        x.split('-tas-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-tas-')) > 1 else 
        x.split('-act-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-act-')) > 1 else 
        x.split('-sa-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-sa-')) > 1 else 
        x.split('-nt-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-nt-')) > 1 else 
        x.split('-wa-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-wa-')) > 1 else 
        x.split('-vic-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-vic-')) > 1 else ''
        )

    #get suburb
    XML_gz_Dataset['suburb']=XML_gz_Dataset['url'].apply(lambda x:
        x.split('-nsw-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-nsw-')) > 1 else
        x.split('-qld-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-qld-')) > 1 else
        x.split('-tas-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-tas-')) > 1 else
        x.split('-act-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-act-')) > 1 else
        x.split('-sa-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-sa-')) > 1 else
        x.split('-nt-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-nt-')) > 1 else
        x.split('-wa-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-wa-')) > 1 else
        x.split('-vic-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-vic-')) > 1 else ''
        )

    #get prop id
    XML_gz_Dataset['prop_id']=XML_gz_Dataset['url'].apply(lambda x:
        x.split('-')[-1]
    )
    XML_gz_Dataset.to_csv(gz_save_path + '/parsed_csv/' + _xml_save[:-3] + '_results' +'.csv')
    print("file saved to: " + gz_save_path + '\\parsed_csv\\' + _xml_save[:-3] + '_results' +'.csv')
    XML_gz_Dataset['lastmod']=pd.to_datetime(XML_gz_Dataset['lastmod'])
    print("total time:", time.time() - _time)


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
   
sitemap_dag = DAG(
        dag_id='use_getXML_Scrape'
        ,default_args=default_args
        ,schedule_interval=None
        ,start_date=days_ago(1)
        ,tags=['get_xml_scrape']
    )
sitemap_starter = DummyOperator( dag = sitemap_dag, task_id='dummy_starter' )
sitemap_ender = DummyOperator( dag = sitemap_dag, task_id='dummy_ender' )
scrape_task = PythonOperator(
    task_id="scrape_sitemap_rawxml"
    ,provide_context=True
    ,op_kwargs={
        'baseurl':'https://www.realestate.com.au/xml-sitemap/'
        # , 'RootDir': '/opt/airflow/logs/XML_save_folder' 
        , 'PageSaveFolder': '/opt/airflow/logs/XML_save_folder/raw_sitemap/'
        ,'Scrapewait': 5
        }
    ,python_callable=ScrapeURL
    ,dag = sitemap_dag
    )
sitemap_starter >> scrape_task >> sitemap_ender 

_max_name=''
_max_path=''
_max_mod= 0

# https://stackoverflow.com/questions/52558018/airflow-generate-dynamic-tasks-in-single-dag-task-n1-is-dependent-on-taskn
for x in os.scandir('/opt/airflow/logs/XML_save_folder/raw_sitemap'):
    if os.path.getmtime(x.path) > _max_mod:
        _max_name=x.name
        _max_path=x.path
        _max_mod=os.path.getmtime(x.path)

xml_parse_dag = DAG(
        dag_id='use_get_xml_parse'
        ,default_args=default_args
        ,schedule_interval=None
        ,start_date=days_ago(1)
        ,tags=['get_xml_parse']
    )
xml_parse_starter = DummyOperator( dag = xml_parse_dag, task_id='dummy_starter' )
xml_parse_ender = DummyOperator( dag = xml_parse_dag, task_id='dummy_ender' )

# if x.name == 'XML_scrape_' + (datetime.datetime.now()).strftime('%Y-%m-%d') +'.csv':
XML_H_Dataset= pd.read_csv(_max_path) 
# for i in range(0, XML_H_Dataset[XML_H_Dataset['filetype'].notnull()].shape[0]):
for i in range(0, XML_H_Dataset[XML_H_Dataset['filetype'].notnull()].iloc[0:5].shape[0]): #
# XMLDataset[XMLDataset['filetype'].notnull()].shape[0]): 
    xml_gz_extract=PythonOperator(
            task_id='scrape_sitemap_gz_'+str(i)
            ,provide_context=True
            ,op_kwargs={
                'baseurl': 'https://www.realestate.com.au/xml-sitemap/'
                , 'PageSaveFolder': '/opt/airflow/logs/XML_save_folder/gz_files/'
                , 'ScrapeFile': XML_H_Dataset[XML_H_Dataset['filetype'].notnull()]['s_filename'].iloc[i:i + 1].to_string(index=False).strip() #pass in filename from filtered iteration
                ,'Scrapewait': 5
                }
            ,python_callable=SaveScrape
            ,dag=xml_parse_dag
            )
    xml_parse_starter >> xml_gz_extract >> xml_parse_ender #a[i]
        # starter >> scrape_task 