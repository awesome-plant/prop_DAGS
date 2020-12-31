# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
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
import sys
import psycopg2


# %%
baseurl='https://www.realestate.com.au/xml-sitemap/'
PageSaveFolder=r'D:\shared_folder\airflow_logs\XML_save_folder\raw_sitemap\\' #'/opt/airflow/logs/XML_save_folder/raw_sitemap'


# %%
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
print("file saved to: " + xmlFile +'.xml')
H_FileSize=round(os.path.getsize(xmlFile +'.xml') / 1000)


# %%
#write to parent table, apparently im storing in 3rd normal
XML_H_Dataset=pd.DataFrame(columns =['external_ip', 'folderpath', 'h_filename', 'scrape_dt', 'h_filesize_kb','h_fileid'])
# XML_H_Dataset.dtypes
XML_H_Dataset=XML_H_Dataset.append({
                'external_ip': str(H_external_ip)
                , 'folderpath': str(PageSaveFolder)
                , 'h_filename': str(XMLsaveFile + '.xml')
                , 'scrape_dt': H_ScrapeDT
                , 'h_filesize_kb': round(int(H_FileSize) / 1000)
                } ,ignore_index=True) 
XML_H_Dataset['h_fileid']=pd.to_numeric(XML_H_Dataset['h_fileid'])
# XML_H_Dataset.to_datetime()


# %%

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
        _LastModified=datetime.datetime.strptime(element.text, '%Y-%m-%dT%H:%M:%S.%f%z')
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
#used in next part
# return XML_S_Dataset


# %%
#next we need to get the next IDs for inserting purposes 
from psycopg2 import Error
try:
    # Connect to an existing database
    connection = psycopg2.connect(user="postgres",
                                  password="root",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="scrape_db")

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


# %%
#now we apply it to the DFs we have 
h_fileid[0]
XML_H_Dataset['h_fileid']=h_fileid[0]
XML_S_Dataset['h_fileid']=h_fileid[0]


# %%
XML_H_Dataset.dtypes


# %%
# https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method
import csv
from io import StringIO

from sqlalchemy import create_engine

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


# %%
engine = create_engine('postgresql://postgres:root@localhost:5432/scrape_db')
XML_H_Dataset.to_sql(
    name='sc_source_header'
    ,con=engine
    ,method=psql_insert_copy
    ,schema='sc_land'
    ,if_exists='append'
    ,index=False
    )

XML_S_Dataset.to_sql(
    name='sc_source_file'
    ,con=engine
    ,method=psql_insert_copy
    ,schema='sc_land'
    ,if_exists='append'
    ,index=False
    )


# %%
XML_S_Dataset.dtypes


# %%



