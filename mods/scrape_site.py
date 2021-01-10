from lxml import etree
import os
import time
import datetime 
import pandas as pd
import numpy as np
import mods.proxy as proxy 
import requests
import gzip
import shutil
import psycopg2
import mods.db_import as db_import 
from sqlalchemy import create_engine
import sys

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def SaveScrape(baseurl, PageSaveFolder, ScrapeFile, Scrapewait, **kwargs):
    _start = time.time()
    XMLsaveFile="XML_scrape_" + (datetime.datetime.now()).strftime('%Y-%m-%d')
    xmlFile=PageSaveFolder + XMLsaveFile 
    with open(xmlFile +'.xml', "w") as saveXML:
        print("done")

    XMLsaveFile="XML_scrape_" + (datetime.datetime.now()).strftime('%Y-%m-%d')
    # time.sleep(random.randint(1,10))
    # ua = UserAgent()
    #headers = {'User-Agent':str(ua.random)}
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}

    r_proxy,prox_status=proxy.getProxy(ps_user="postgres", ps_pass="root", ps_host="172.22.114.65",ps_port="5432", ps_db="scrape_db", update=True)
 
    if prox_status==False: 
        print('error getting proxy, quitting')
        sys.exit() 

    # https://stackoverflow.com/questions/23013220/max-retries-exceeded-with-url-in-requests
    # scrape_pass=False    
    # while scrape_pass==False:
    #     try:
    session = requests.Session()
    retry = Retry(connect=5, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.get(url)
    response = session.get(baseurl + ScrapeFile,headers=headers, timeout=Scrapewait, proxies= {'http' : 'http://' + r_proxy, 'https' : 'https://' + r_proxy}) #r_proxy)
        #     scrape_pass=True
        # except: 

    print('gz file:', ScrapeFile)
    gz_save_name =ScrapeFile[:-7] + '_' + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.gz'

    #save to gz
    open(PageSaveFolder + gz_save_name, 'wb').write(response.content)
    print("written to dir:", PageSaveFolder + gz_save_name)
        #feast upon that rich gooey xml 
    _xml_save = ScrapeFile[:-7] + '_' + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.xml'  
    with gzip.open(PageSaveFolder + gz_save_name, 'rb') as f_in:
        with open(PageSaveFolder + _xml_save, 'wb') as f_out: 
            shutil.copyfileobj(f_in, f_out)
    tree = etree.parse(PageSaveFolder + _xml_save)
    with open(PageSaveFolder + _xml_save, "wb") as saveXML:
        saveXML.write(etree.tostring(tree,pretty_print=True))

    body=tree.xpath('//ns:url',namespaces={'ns':"http://www.sitemaps.org/schemas/sitemap/0.9"})
    _count=1
    _time=time.time()

    #now we parse and read, using lists instead of df since its A BUNCH faster
    list_lastmod=[]
    list_url=[]
    list_state=[]
    list_proptype=[]
    list_suburb=[]
    list_propid=[]
    for element in body:
        if _count % 10000 == 0: 
            print("interval:", str(_count-1)," -total runtime:", time.time()-_time)

        list_lastmod.append(element[1].text)
        list_url.append(element[0].text)
        if '-nsw-' in element[0].text: 
            list_state.append('nsw')
            list_proptype.append( (element[0].text).split('-nsw-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') )
            list_suburb.append( (element[0].text).split('-nsw-')[1].replace('https://www.realestate.com.au/property-','').replace((element[0].text).split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() )
        elif '-qld-' in element[0].text: 
            list_state.append('qld')
            list_proptype.append( (element[0].text).split('-qld-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') )
            list_suburb.append( (element[0].text).split('-qld-')[1].replace('https://www.realestate.com.au/property-','').replace((element[0].text).split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() )
        elif '-tas-' in element[0].text: 
            list_state.append('tas')
            list_proptype.append( (element[0].text).split('-tas-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') )
            list_suburb.append( (element[0].text).split('-tas-')[1].replace('https://www.realestate.com.au/property-','').replace((element[0].text).split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() )
        elif '-act-' in element[0].text: 
            list_state.append('act')
            list_proptype.append( (element[0].text).split('-act-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') )
            list_suburb.append( (element[0].text).split('-act-')[1].replace('https://www.realestate.com.au/property-','').replace((element[0].text).split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() )
        elif '-sa-' in element[0].text: 
            list_state.append('sa')
            list_proptype.append( (element[0].text).split('-sa-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') )
            list_suburb.append( (element[0].text).split('-sa-')[1].replace('https://www.realestate.com.au/property-','').replace((element[0].text).split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() )
        elif '-nt-' in element[0].text: 
            list_state.append('nt')
            list_proptype.append( (element[0].text).split('-nt-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') )
            list_suburb.append( (element[0].text).split('-nt-')[1].replace('https://www.realestate.com.au/property-','').replace((element[0].text).split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() )
        elif '-wa-' in element[0].text: 
            list_state.append('wa')
            list_proptype.append( (element[0].text).split('-wa-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') )
            list_suburb.append( (element[0].text).split('-wa-')[1].replace('https://www.realestate.com.au/property-','').replace((element[0].text).split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() )
        elif '-vic-' in element[0].text: 
            list_state.append('vic')
            list_proptype.append( (element[0].text).split('-vic-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') )
            list_suburb.append( (element[0].text).split('-vic-')[1].replace('https://www.realestate.com.au/property-','').replace((element[0].text).split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() )
        else: 
            list_url.append('')
            list_state.append('')
            list_suburb.append('')

        list_propid.append( (element[0].text).split('-')[-1] )
        _count+=1 

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
    print("inserting into tables: sc_property_links")
    engine = create_engine('postgresql://postgres:root@172.22.114.65:5432/scrape_db')
    XML_gz_Dataset.to_sql(
        name='sc_property_links'
        ,schema='sc_land'
        ,con=engine
        ,method=db_import.psql_insert_copy
        ,if_exists='append'
        ,index=False
        )
    print("insert complete")
    print('removing extracted xml file')
    os.remove(PageSaveFolder + _xml_save)
    print("fin")

    print("total runtime", time.time() - _time)
    return r_proxy