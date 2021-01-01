{
 "metadata": {
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.6-final"
  },
  "orig_nbformat": 2,
  "kernelspec": {
   "name": "python3",
   "display_name": "Python 3",
   "language": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2,
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "metadata": {},
   "outputs": [],
   "source": [
    "import logging\n",
    "import os\n",
    "import datetime \n",
    "import time\n",
    "import requests\n",
    "import urllib.request\n",
    "from lxml import etree\n",
    "from fake_useragent import UserAgent\n",
    "import pandas as pd \n",
    "import gzip\n",
    "import shutil \n",
    "import sys\n",
    "import psycopg2"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [],
   "source": [
    "baseurl='https://www.realestate.com.au/xml-sitemap/'\n",
    "PageSaveFolder=r'D:\\shared_folder\\airflow_logs\\XML_save_folder\\raw_sitemap\\\\' #'/opt/airflow/logs/XML_save_folder/raw_sitemap'"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {},
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "text": [
      "file saved to: D:\\shared_folder\\airflow_logs\\XML_save_folder\\raw_sitemap\\\\XML_scrape_2020-12-31.xml\n"
     ]
    }
   ],
   "source": [
    "XMLsaveFile=\"XML_scrape_\" + (datetime.datetime.now()).strftime('%Y-%m-%d')\n",
    "ua = UserAgent()\n",
    "headers = {'User-Agent':str(ua.random)}\n",
    "\n",
    "#get external IP https://stackoverflow.com/questions/2311510/getting-a-machines-external-ip-address-with-python\n",
    "H_external_ip = urllib.request.urlopen('https://ident.me').read().decode('utf8')\n",
    "H_ScrapeDT=(datetime.datetime.now())\n",
    "# headers = { 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36', }\n",
    "response = requests.get(baseurl,headers=headers)\n",
    "xmlFile=PageSaveFolder + XMLsaveFile \n",
    "\n",
    "# time.sleep(600)\n",
    "saveXML=open(xmlFile +'.xml', \"w\")\n",
    "saveXML.write(response.text)\n",
    "saveXML.close()\n",
    "print(\"file saved to: \" + xmlFile +'.xml')\n",
    "H_FileSize=round(os.path.getsize(xmlFile +'.xml') / 1000)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {},
   "outputs": [],
   "source": [
    "#write to parent table, apparently im storing in 3rd normal\n",
    "XML_H_Dataset=pd.DataFrame(columns =['external_ip', 'folderpath', 'h_filename', 'scrape_dt', 'h_filesize_kb','h_fileid'])\n",
    "# XML_H_Dataset.dtypes\n",
    "XML_H_Dataset=XML_H_Dataset.append({\n",
    "                'external_ip': str(H_external_ip)\n",
    "                , 'folderpath': str(PageSaveFolder)\n",
    "                , 'h_filename': str(XMLsaveFile + '.xml')\n",
    "                , 'scrape_dt': H_ScrapeDT\n",
    "                , 'h_filesize_kb': int(H_FileSize)\n",
    "                } ,ignore_index=True) \n",
    "XML_H_Dataset['h_fileid']=pd.to_numeric(XML_H_Dataset['h_fileid'])\n",
    "# XML_H_Dataset.to_datetime()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {},
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "text": [
      "file saved to: D:\\shared_folder\\airflow_logs\\XML_save_folder\\raw_sitemap\\\\XML_scrape_2020-12-31.csv\n"
     ]
    }
   ],
   "source": [
    "\n",
    "#parse to XML \n",
    "result = response.content \n",
    "root = etree.fromstring(result) \n",
    "#scrape variables\n",
    "_Suffix=_Filename=_LastModified=_Size=_StorageClass=_Type=\"\"\n",
    "XML_S_Dataset=pd.DataFrame(columns =['suffix', 's_filename', 'filetype', 'lastmod', 's_filesize_kb', 'storageclass', 'h_fileid'])\n",
    "\n",
    "#iteration is done literally one aspect at a time, since xml wouldnt play nice\n",
    "#print element.tag to understand\n",
    "for element in root.iter():\n",
    "    if str(element.tag).replace(\"{http://s3.amazonaws.com/doc/2006-03-01/}\",\"\") == 'Contents' and _Filename != '':\n",
    "        #write to pd \n",
    "        XML_S_Dataset=XML_S_Dataset.append({\n",
    "                 'suffix' : str(_Suffix)\n",
    "                , 's_filename' : str(_Filename)\n",
    "                , 'filetype' : str(_Type)\n",
    "                , 'lastmod': _LastModified\n",
    "                , 's_filesize_kb' : round(int(_Size) / 1000)\n",
    "                , 'storageclass': str(_StorageClass)\n",
    "                } ,ignore_index=True) \n",
    "        _Suffix=_Filename=_LastModified=_Size=_StorageClass=_Type=\"\"\n",
    "    elif str(element.tag).replace(\"{http://s3.amazonaws.com/doc/2006-03-01/}\",\"\") == 'Key':\n",
    "        _Filename=str(element.text)\n",
    "        _Suffix=str(element.text).split('-')[0]\n",
    "        #get name subcat\n",
    "        if 'buy' in _Filename.lower(): \n",
    "            _Type='buy'\n",
    "        elif 'sold' in _Filename.lower(): \n",
    "            _Type='sold' \n",
    "        elif 'rent' in _Filename.lower(): \n",
    "            _Type='rent' \n",
    "        else: _Type=''\n",
    "    elif str(element.tag).replace(\"{http://s3.amazonaws.com/doc/2006-03-01/}\",\"\") == 'LastModified':\n",
    "        _LastModified=datetime.datetime.strptime(element.text, '%Y-%m-%dT%H:%M:%S.%f%z')\n",
    "    elif str(element.tag).replace(\"{http://s3.amazonaws.com/doc/2006-03-01/}\",\"\") == 'Size':\n",
    "        _Size=str(element.text)\n",
    "    elif str(element.tag).replace(\"{http://s3.amazonaws.com/doc/2006-03-01/}\",\"\") == 'StorageClass':\n",
    "        _StorageClass=str(element.text)\n",
    "#hard code dtypes for sql later        \n",
    "XML_S_Dataset['lastmod']=pd.to_datetime(XML_S_Dataset['lastmod'])\n",
    "XML_S_Dataset['s_filesize_kb']=pd.to_numeric(XML_S_Dataset['s_filesize_kb'])\n",
    "XML_S_Dataset['h_fileid']=pd.to_numeric(XML_S_Dataset['h_fileid'])\n",
    "XML_S_Dataset.to_csv(xmlFile +'.csv')\n",
    "print(\"file saved to: \" + xmlFile +'.csv')\n",
    "#used in next part\n",
    "# return XML_S_Dataset"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "metadata": {},
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "text": [
      "Error while connecting to PostgreSQL name 'psycopg2' is not defined\n"
     ]
    },
    {
     "output_type": "error",
     "ename": "NameError",
     "evalue": "name 'connection' is not defined",
     "traceback": [
      "\u001b[1;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[1;31mNameError\u001b[0m                                 Traceback (most recent call last)",
      "\u001b[1;32m<ipython-input-2-d283c81e41b3>\u001b[0m in \u001b[0;36m<module>\u001b[1;34m\u001b[0m\n\u001b[0;32m     16\u001b[0m     \u001b[0mprint\u001b[0m\u001b[1;33m(\u001b[0m\u001b[1;34m\"Error while connecting to PostgreSQL\"\u001b[0m\u001b[1;33m,\u001b[0m \u001b[0merror\u001b[0m\u001b[1;33m)\u001b[0m\u001b[1;33m\u001b[0m\u001b[1;33m\u001b[0m\u001b[0m\n\u001b[0;32m     17\u001b[0m \u001b[1;32mfinally\u001b[0m\u001b[1;33m:\u001b[0m\u001b[1;33m\u001b[0m\u001b[1;33m\u001b[0m\u001b[0m\n\u001b[1;32m---> 18\u001b[1;33m     \u001b[1;32mif\u001b[0m \u001b[1;33m(\u001b[0m\u001b[0mconnection\u001b[0m\u001b[1;33m)\u001b[0m\u001b[1;33m:\u001b[0m\u001b[1;33m\u001b[0m\u001b[1;33m\u001b[0m\u001b[0m\n\u001b[0m\u001b[0;32m     19\u001b[0m         \u001b[0mcursor\u001b[0m\u001b[1;33m.\u001b[0m\u001b[0mclose\u001b[0m\u001b[1;33m(\u001b[0m\u001b[1;33m)\u001b[0m\u001b[1;33m\u001b[0m\u001b[1;33m\u001b[0m\u001b[0m\n\u001b[0;32m     20\u001b[0m         \u001b[0mconnection\u001b[0m\u001b[1;33m.\u001b[0m\u001b[0mclose\u001b[0m\u001b[1;33m(\u001b[0m\u001b[1;33m)\u001b[0m\u001b[1;33m\u001b[0m\u001b[1;33m\u001b[0m\u001b[0m\n",
      "\u001b[1;31mNameError\u001b[0m: name 'connection' is not defined"
     ]
    }
   ],
   "source": [
    "\n",
    "from psycopg2 import Error\n",
    "try:\n",
    "    # Connect to an existing database\n",
    "    connection = psycopg2.connect(user=\"postgres\",password=\"root\",host=\"172.22.114.65\",port=\"5432\",database=\"scrape_db\")\n",
    "\n",
    "    cursor = connection.cursor()\n",
    "    cursor.execute(\"SELECT coalesce(max(H_FILEID), 0) + 1 as h_fileid from sc_land.SC_SOURCE_HEADER\")\n",
    "    h_fileid = cursor.fetchone() #next iteration of file ID \n",
    "\n",
    "    print('new h_fileid is:', h_fileid[0])\n",
    "except (Exception, Error) as error:\n",
    "    print(\"Error while connecting to PostgreSQL\", error)\n",
    "finally:\n",
    "    if (connection):\n",
    "        cursor.close()\n",
    "        connection.close()\n",
    "        print(\"PostgreSQL connection is closed\")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {},
   "outputs": [],
   "source": [
    "#now we apply it to the DFs we have \n",
    "h_fileid[0]\n",
    "XML_H_Dataset['h_fileid']=h_fileid[0]\n",
    "XML_S_Dataset['h_fileid']=h_fileid[0]"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {},
   "outputs": [
    {
     "output_type": "execute_result",
     "data": {
      "text/plain": [
       "external_ip              object\n",
       "folderpath               object\n",
       "h_filename               object\n",
       "scrape_dt        datetime64[ns]\n",
       "h_filesize_kb            object\n",
       "h_fileid                  int64\n",
       "dtype: object"
      ]
     },
     "metadata": {},
     "execution_count": 8
    }
   ],
   "source": [
    "XML_H_Dataset.dtypes"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {},
   "outputs": [],
   "source": [
    "# https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method\n",
    "import csv\n",
    "from io import StringIO\n",
    "\n",
    "from sqlalchemy import create_engine\n",
    "\n",
    "def psql_insert_copy(table, conn, keys, data_iter):\n",
    "    \"\"\"\n",
    "    Execute SQL statement inserting data\n",
    "\n",
    "    Parameters\n",
    "    ----------\n",
    "    table : pandas.io.sql.SQLTable\n",
    "    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection\n",
    "    keys : list of str\n",
    "        Column names\n",
    "    data_iter : Iterable that iterates the values to be inserted\n",
    "    \"\"\"\n",
    "    # gets a DBAPI connection that can provide a cursor\n",
    "    dbapi_conn = conn.connection\n",
    "    with dbapi_conn.cursor() as cur:\n",
    "        s_buf = StringIO()\n",
    "        writer = csv.writer(s_buf)\n",
    "        writer.writerows(data_iter)\n",
    "        s_buf.seek(0)\n",
    "\n",
    "        columns = ', '.join('\"{}\"'.format(k) for k in keys)\n",
    "        if table.schema:\n",
    "            table_name = '{}.{}'.format(table.schema, table.name)\n",
    "        else:\n",
    "            table_name = table.name\n",
    "\n",
    "        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(\n",
    "            table_name, columns)\n",
    "        cur.copy_expert(sql=sql, file=s_buf)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "metadata": {},
   "outputs": [],
   "source": [
    "engine = create_engine('postgresql://postgres:root@localhost:5432/scrape_db')\n",
    "XML_H_Dataset.to_sql(\n",
    "    name='sc_source_header'\n",
    "    ,con=engine\n",
    "    ,method=psql_insert_copy\n",
    "    ,schema='sc_land'\n",
    "    ,if_exists='append'\n",
    "    ,index=False\n",
    "    )\n",
    "\n",
    "XML_S_Dataset.to_sql(\n",
    "    name='sc_source_file'\n",
    "    ,con=engine\n",
    "    ,method=psql_insert_copy\n",
    "    ,schema='sc_land'\n",
    "    ,if_exists='append'\n",
    "    ,index=False\n",
    "    )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "metadata": {},
   "outputs": [
    {
     "output_type": "execute_result",
     "data": {
      "text/plain": [
       "suffix                        object\n",
       "s_filename                    object\n",
       "filetype                      object\n",
       "lastmod          datetime64[ns, UTC]\n",
       "s_filesize_kb                  int64\n",
       "storageclass                  object\n",
       "h_fileid                       int64\n",
       "dtype: object"
      ]
     },
     "metadata": {},
     "execution_count": 11
    }
   ],
   "source": [
    "XML_S_Dataset.dtypes"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ]
}