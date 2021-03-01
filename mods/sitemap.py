import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import db_import as db_import
import proxy as proxy 
import argparse #add flags here

def site_ScrapeParentURL():  
    # from selenium.webdriver.chrome.options import Options
    # from selenium import webdriver
    # from selenium.webdriver import ActionChains
    # from selenium.webdriver.common.proxy import Proxy, ProxyType
    from lxml import etree
    import pandas as pd
    import datetime 
    import numpy as np
    import random
    import requests 
    import time 

    scrape_status=False
    site_url='https://www.realestate.com.au/xml-sitemap/'
    timeout=5
    loopcount=0
    prox, proxy_type = db_import.getDBProxy(
                ps_user="postgres"
                , ps_pass="root"
                , ps_host="172.22.114.65"
                , ps_port="5432"
                , ps_db="scrape_db"
                , update='sitemap'
                ) 
    proxies={}
    for pt in proxy_type.split(';'): #build dict dynamically 
        if pt =='http': proxies.update({pt : pt + '://' + prox})
        elif pt =='https': proxies.update({pt : pt + '://' + prox})
        elif ( pt =='socks4'or pt=='socks5'): proxies.update({'http' : pt + '://' + prox, 'https' : pt + '://' + prox,})
    while scrape_status==False: #do until done
        try:
            if loopcount>=10: #could be a proxy issue 
                print("10 failures, new IP time")
                loopcount=0
                prox, proxy_type = db_import.getDBProxy(
                ps_user="postgres"
                , ps_pass="root"
                , ps_host="172.22.114.65"
                , ps_port="5432"
                , ps_db="scrape_db"
                , update='sitemap'
                ) 
                proxies={}
                for pt in proxy_type.split(';'): #build dict dynamically 
                    if pt =='http': proxies.update({pt : pt + '://' + prox})
                    elif pt =='https': proxies.update({pt : pt + '://' + prox})
                    elif ( pt =='socks4'or pt=='socks5'): proxies.update({'http' : pt + '://' + prox, 'https' : pt + '://' + prox,})
            loopcount+=1 
            headers={ 'User-Agent': proxy.getHeader(random.randint(0,249))  } 
            r = requests.get(site_url, proxies=proxies,headers=headers, timeout=timeout,verify=False)
            if len(r.text) > 50:
                scrape_status=True
                root = etree.fromstring(r.text)
                body=root.xpath('//ns:Contents',namespaces={'ns':"http://s3.amazonaws.com/doc/2006-03-01/"})
                print('body is:', str(body))
            elif len(r.text) < 50: 
                print("bot blocked IP:",proxy, "lc:", str(loopcount))
        except Exception as e:
            print("prox:", str(proxies), "lc:",str(loopcount), str("-error:", str(e) )
    #now we read/parse the xml
    #header link ref
    XML_H_Dataset=pd.DataFrame({ 
        'external_ip': str(proxies)
        , 'h_filename': str("XML_scrape_" + (datetime.datetime.now()).strftime('%Y-%m-%d'))
        , 'scrape_dt': (datetime.datetime.now())
        },index=[0]
    )

    l_suffix=[]
    l_filename=[]
    l_filetype=[]
    l_lastmod=[]
    l_filesize_kb=[]
    l_storageclass=[]
    for element in body:
        _Type=''
        if 'buy' in (element[0].text).lower(): 
            _Type='buy'
        elif 'sold' in (element[0].text).lower(): 
            _Type='sold'
        elif 'rent' in (element[0].text).lower(): 
            _Type='rent'
        l_suffix.append(str(element[0].text).split('-')[0])
        l_filename.append(str(element[0].text))
        l_filetype.append(_Type)
        l_lastmod.append(datetime.datetime.strptime((element[1].text).replace('Z','+0000'), '%Y-%m-%dT%H:%M:%S.%f%z') )
        l_filesize_kb.append(round(int(element[3].text) / 1000))
        l_storageclass.append(str(element[4].text))

    XML_S_Dataset = pd.DataFrame(
        np.column_stack([l_suffix, l_filename, l_filetype, l_lastmod, l_filesize_kb, l_storageclass]), 
        columns=['suffix','s_filename', 'filetype', 'lastmod', 's_filesize_kb', 'storageclass'])

    print('first')
    print(XML_S_Dataset.head())
    XML_S_Dataset['lastmod']=pd.to_datetime(XML_S_Dataset['lastmod'])
    XML_S_Dataset['s_filesize_kb']=pd.to_numeric(XML_S_Dataset['s_filesize_kb'])

    print('second')
    print(XML_S_Dataset.head())
    fileID=db_import.getFileID()
    XML_S_Dataset['h_fileid']=fileID
    XML_H_Dataset['h_fileid']=fileID
    XML_S_Dataset['h_fileid']=pd.to_numeric(XML_S_Dataset['h_fileid'])
    XML_H_Dataset['h_fileid']=pd.to_numeric(XML_H_Dataset['h_fileid'])

    print('third')
    print(XML_S_Dataset.head())

    print("inserting into tables: sc_source_header, sc_source_file")
    db_import.insertData(ps_user="postgres", ps_pass="root", ps_host="172.22.114.65", ps_port="5432", ps_db="scrape_db", table='sc_source_header', df_insert=XML_H_Dataset)
    db_import.insertData(ps_user="postgres", ps_pass="root", ps_host="172.22.114.65", ps_port="5432", ps_db="scrape_db", table='sc_source_file', df_insert=XML_S_Dataset)
    print("inserts completed, fileid:", str(fileID), "-proxy:", str(proxies))

# def SaveScrape(baseurl, PageSaveFolder, ScrapeFile, Scrapewait, **kwargs):
#     print('gz file:', ScrapeFile)
#     time.sleep(random.randint(1,10))
#     # ua = UserAgent()
#     #headers = {'User-Agent':str(ua.random)}
#     headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
#     proxies,external_ip=proxy.getProxy()

#     _getPass=False
#         #loop until it works, if it takes too long get another proxy 
#     while _getPass==False:
#         try:
#             response = requests.get(baseurl + ScrapeFile, headers=headers,proxies=proxies, timeout=Scrapewait)
#             _getPass=True
#         except Exception as e:
#             print('error recieved, trying again:',e) 
#             proxies,external_ip=proxy.getProxy()

#     print('gz file:', ScrapeFile)
#     #download from sitemap, use dynamic variable 
#     sitemap_url = baseurl #'https://www.realestate.com.au/xml-sitemap/'#pdp-sitemap-buy-1.xml.gz' 
#     _file=ScrapeFile #im lazy, sue me
#     gz_save_name =_file[:-7] + '_' + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.gz'
#     gz_url = sitemap_url + _file
#     gz_save_path = PageSaveFolder
#     #save to gz
#     open(gz_save_path + gz_save_name, 'wb').write(response.content)

#     #save gz to dir for archiving 
#     print("file:", gz_save_name)
#     print("written to dir:", gz_save_path + gz_save_name)
#     #feast upon that rich gooey xml 
#     _xml_save = _file[:-7] + '_' + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.xml'  
#     with gzip.open(gz_save_path + gz_save_name, 'rb') as f_in:
#         with open(gz_save_path + _xml_save, 'wb') as f_out: 
#             shutil.copyfileobj(f_in, f_out)
#     tree = etree.parse(gz_save_path + _xml_save)
#     with open(gz_save_path + _xml_save, "wb") as saveXML:
#         saveXML.write(etree.tostring(tree,pretty_print=True))

#     _count=1
#     _time=time.time()
#     #iterate xml
#     body=tree.xpath('//ns:url',namespaces={'ns':"http://www.sitemaps.org/schemas/sitemap/0.9"})
#     _count=1
#     _time=time.time()
#     # XML_gz_Dataset=pd.DataFrame(columns =['parent_gz','scrape_dt','url', 'proptype', 'state', 'suburb', 'prop_id', 'lastmod', 'external_ip', 's_fileid'])
#     new_Dataset=   pd.DataFrame(columns =['parent_gz','scrape_dt','url', 'lastmod', 's_fileid'])
#     for element in body:
#         if _count % 10000 == 0: 
#             print("interval:", str(_count-1)," -total runtime:", time.time()-_time)
#         # _LastMod = element[1].text
#         # _Url = element[0].text
#         # #writes results to df, same as the previous module 
#         new_Dataset=new_Dataset.append({
#                     'parent_gz': str(ScrapeFile)
#                     ,'scrape_dt' : (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
#                     , 'url' : str(element[0].text)
#                     , 'lastmod': str(element[1].text)
#                     } ,ignore_index=True) 
#         _count+=1 
#     print("xml extract time:", time.time() - _time)
#     XML_gz_Dataset=new_Dataset

#     #add state
#     XML_gz_Dataset['state']=XML_gz_Dataset.apply(lambda x: 
#         'nsw' if '-nsw-' in x.url else
#         'qld' if '-qld-' in x.url else
#         'tas' if '-tas-' in x.url else
#         'act' if '-act-' in x.url else
#         'sa' if '-sa-' in x.url else
#         'nt' if '-nt-' in x.url else
#         'wa' if '-wa-' in x.url else
#         'vic' if '-vic-' in x.url else ''
#         , axis=1)

#     # get proptype 
#     XML_gz_Dataset['proptype']=XML_gz_Dataset['url'].apply(lambda x: 
#         x.split('-nsw-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-nsw-')) > 1 else 
#         x.split('-qld-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-qld-')) > 1 else 
#         x.split('-tas-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-tas-')) > 1 else 
#         x.split('-act-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-act-')) > 1 else 
#         x.split('-sa-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-sa-')) > 1 else 
#         x.split('-nt-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-nt-')) > 1 else 
#         x.split('-wa-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-wa-')) > 1 else 
#         x.split('-vic-')[0].replace('https://www.realestate.com.au/property-','').replace('+', ' ') if len(x.split('-vic-')) > 1 else ''
#         )

#     #get suburb
#     XML_gz_Dataset['suburb']=XML_gz_Dataset['url'].apply(lambda x:
#         x.split('-nsw-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-nsw-')) > 1 else
#         x.split('-qld-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-qld-')) > 1 else
#         x.split('-tas-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-tas-')) > 1 else
#         x.split('-act-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-act-')) > 1 else
#         x.split('-sa-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-sa-')) > 1 else
#         x.split('-nt-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-nt-')) > 1 else
#         x.split('-wa-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-wa-')) > 1 else
#         x.split('-vic-')[1].replace('https://www.realestate.com.au/property-','').replace(x.split('-')[-1],'').replace('-',' ').replace('+', ' ').strip() if len(x.split('-vic-')) > 1 else ''
#         )

#     #get prop id
#     XML_gz_Dataset['prop_id']=XML_gz_Dataset['url'].apply(lambda x:
#         x.split('-')[-1]
#     )
#     XML_gz_Dataset.to_csv(gz_save_path + '/parsed_csv/' + _xml_save[:-3] + '_results' +'.csv')
#     print("file saved to: " + gz_save_path + '\\parsed_csv\\' + _xml_save[:-3] + '_results' +'.csv')
#     XML_gz_Dataset['lastmod']=pd.to_datetime(XML_gz_Dataset['lastmod'])
#     print("total time:", time.time() - _time)


#     #now we add to db table 
#     #parent file link
#     connection = psycopg2.connect(user="postgres",password="root",host="172.22.114.65",port="5432",database="scrape_db")
#     cursor = connection.cursor()
#     # with connection.cursor() as cursor:
#     cursor.execute("""
#         select max(s_fileid)
#         FROM sc_land.sc_source_file
#         WHERE s_filename = %(s_filename)s
#         and date(lastmod) = %(lastmod)s;
#         """,
#             {
#                 's_filename': XML_gz_Dataset['parent_gz'].drop_duplicates()[0]
#                 ,'lastmod' : XML_gz_Dataset['lastmod'].dt.date.drop_duplicates()[0]
#             }
#         )
#     result = cursor.fetchone()
#     print("parent file link is:",ScrapeFile,"is:", result[0])
#     XML_gz_Dataset['s_fileid']=result[0]
#     #remove redundant link
#     XML_gz_Dataset=XML_gz_Dataset.drop(columns=['parent_gz'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-mod') #module
    args = vars(parser.parse_args())
    if args['mod'] =='scrape_parent': #args['mod'] =='openproxy':
        print("running scrape_url")
        site_ScrapeParentURL()