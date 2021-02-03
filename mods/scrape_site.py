from lxml import etree
import os
import time
import datetime 
import pandas as pd
import numpy as np
import mods.proxy as proxy 
import random
import requests
import gzip
import shutil
import psycopg2
import mods.db_import as db_import 
from sqlalchemy import create_engine
import sys

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def SaveScrape(baseurl, PageSaveFolder, ScrapeFile, Scrapewait, useProxy, **kwargs):
    _time=time.time()
    XMLsaveFile="XML_scrape_" + (datetime.datetime.now()).strftime('%Y-%m-%d')
    xmlFile=PageSaveFolder + XMLsaveFile 
    with open(xmlFile +'.xml', "w") as saveXML:
        print("blank xml created")

    XMLsaveFile="XML_scrape_" + (datetime.datetime.now()).strftime('%Y-%m-%d')
    # time.sleep(random.randint(1,10))
    # ua = UserAgent()
    #headers = {'User-Agent':str(ua.random)}
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    if useProxy != '':
        print("using previous proxy:", useProxy)
        r_proxy=useProxy
    elif useProxy == '':
        r_proxy,prox_status=proxy.getProxy(ps_user="postgres", ps_pass="root", ps_host="172.22.114.65",ps_port="5432", ps_db="scrape_db", update=True)
        if prox_status==False: 
            print('error getting proxy, quitting')
            sys.exit() 
        

    # https://stackoverflow.com/questions/23013220/max-retries-exceeded-with-url-in-requests
    
    # session = requests.Session()
    # retry = Retry(connect=5, backoff_factor=0.5)
    # adapter = HTTPAdapter(max_retries=retry)
    # session.mount('http://', adapter)
    # session.mount('https://', adapter)
    # session.get(baseurl)
    _pass=False    
    _loopcount=0
    while _pass==False:
        try:
            response = requests.get(baseurl + ScrapeFile,headers=headers, timeout=Scrapewait, proxies= {'http' : 'http://' + r_proxy, 'https' : 'https://' + r_proxy}) #r_proxy)
            _pass=True
        except: # requests.exceptions.Timeout:
            _waittime=+random.randint(1,9)
            print("count:",_loopcount,"-timeout, wait secs before retry:", _waittime)
            time.sleep(_waittime)
            _loopcount+=1
        if _loopcount >=20: 
            print("getting new proxy after 20 tries, link:",baseurl + ScrapeFile)
            r_proxy,prox_status=proxy.getProxy(ps_user="postgres", ps_pass="root", ps_host="172.22.114.65",ps_port="5432", ps_db="scrape_db", update=True)
            _loopcount=0
    gz_save_name =ScrapeFile[:-7] + '_' + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.gz'

    #save to gz
    open(PageSaveFolder + gz_save_name, 'wb').write(response.content)
    time.sleep(5)
        #feast upon that rich gooey xml 
    _xml_save = ScrapeFile[:-7] + '_' + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.xml'  
    _pass=False
    _loopcount=0
    while _pass==False:
        try:
            with gzip.open(PageSaveFolder + gz_save_name, 'rb') as f_in:
                time.sleep(5)
                with open(PageSaveFolder + _xml_save, 'wb') as f_out: 
                    time.sleep(5)
                    shutil.copyfileobj(f_in, f_out)
            tree = etree.parse(PageSaveFolder + _xml_save)
            with open(PageSaveFolder + _xml_save, "wb") as saveXML:
                saveXML.write(etree.tostring(tree,pretty_print=True))
            _pass=True
        except: 
            _waittime=+random.randint(1,9)
            print("count:",_loopcount,"-error extracting file, wait secs before retry:", _waittime)
            time.sleep(_waittime)
            _loopcount+=1
        if _loopcount==1:
            print("20 tries, aborting")
            sys.exit() 

    body=tree.xpath('//ns:url',namespaces={'ns':"http://www.sitemaps.org/schemas/sitemap/0.9"})
    _count=1
    #now we parse and read, using lists instead of df since its A BUNCH faster
    list_lastmod=[]
    list_url=[]
    list_state=[]
    list_proptype=[]
    list_suburb=[]
    list_propid=[]
    for element in body:
        # if _count % 10000 == 0: 
        #     print("interval:", str(_count-1)," -total runtime:", time.time()-_time)
        list_lastmod.append(element[1].text)
        list_url.append(element[0].text)
        _splitval=''
        if '-nsw-' in element[0].text: _splitval='-nsw-'
        # elif '+nsw+' in element[0].text: _splitval='+nsw+' 
        elif '-qld-' in element[0].text: _splitval='-qld-'
        # elif '+qld+' in element[0].text:  _splitval='+qld+'  
        elif '-tas-' in element[0].text: _splitval='-tas-'
        # elif '+tas+' in element[0].text: _splitval='+tas+'
        elif '-act-' in element[0].text: _splitval='-act-'
        # elif '+act+' in element[0].text: _splitval='+act+'
        elif '-sa-' in element[0].text: _splitval='-sa-'
        # elif '+sa+' in element[0].text: _splitval='+sa+'
        elif '-nt-' in element[0].text: _splitval='-nt-'
        # elif '+nt+' in element[0].text: _splitval='+nt+'
        elif '-wa-' in element[0].text: _splitval='-wa-'
        # elif '+wa+' in element[0].text: _splitval='+wa+'
        elif '-vic-' in element[0].text: _splitval='-vic-'
        # elif '+vic+' in element[0].text: _splitval='+vic+'

        if _splitval !='':
            list_state.append(_splitval.replace('-','').replace('+',''))
            list_proptype.append( (element[0].text).split(_splitval)[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') )
            list_suburb.append( (element[0].text).split(_splitval)[1].replace('https://www.realestate.com.au/property-','').replace((element[0].text).split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() )
        else: 
            list_state.append('')
            list_proptype.append('')
            list_suburb.append('')
        list_propid.append( (element[0].text).split('-')[-1] )
        # _count+=1 

    XML_gz_Dataset = pd.DataFrame(
        np.column_stack([list_lastmod, list_url, list_proptype, list_state, list_suburb, list_propid]), 
        columns=['lastmod', 'url', 'proptype', 'state', 'suburb', 'prop_id'])

    XML_gz_Dataset.to_csv(PageSaveFolder + '/parsed_csv/' + _xml_save[:-3] + '_results' +'.csv')
    print("file saved to: " + PageSaveFolder + '\\parsed_csv\\' + _xml_save[:-3] + '_results' +'.csv')
    XML_gz_Dataset['lastmod']=pd.to_datetime(XML_gz_Dataset['lastmod'])
    print("total xml time:", time.time() - _time)

    XML_gz_Dataset['parent_gz']=XMLsaveFile
    XML_gz_Dataset['scrape_dt']=(datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
    XML_gz_Dataset['lastmod']=pd.to_datetime(XML_gz_Dataset['lastmod'])
    XML_gz_Dataset['external_ip']=r_proxy

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
    # #time to insert  
    engine = create_engine('postgresql://postgres:root@172.22.114.65:5432/scrape_db')
    XML_gz_Dataset.to_sql(
        name='sc_property_links'
        ,schema='sc_land'
        ,con=engine
        ,method=db_import.psql_insert_copy
        ,if_exists='append'
        ,index=False
        )
    os.remove(PageSaveFolder + _xml_save)
    print("total runtime", time.time() - _time)
    print('----------------------------------------------------------------')
    return r_proxy