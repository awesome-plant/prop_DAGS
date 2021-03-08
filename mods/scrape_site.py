from lxml import etree
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import db_import as db_import 
import proxy as proxy 
import time
import datetime 
import pandas as pd
import numpy as np
import random
import requests
import gzip
import shutil
import psycopg2
from sqlalchemy import create_engine
from json import JSONDecoder
import argparse #add flags here

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
    headers={ 'User-Agent': proxy.getHeader(random.randint(0,249))  } 
    if useProxy != '':
        print("using previous proxy:", useProxy)
        r_proxy=useProxy
    elif useProxy == '':
        r_proxy,prox_status=proxy.getProxy(ps_user="postgres", ps_pass="root", ps_host="172.22.114.65",ps_port="5432", ps_db="scrape_db", update=True)
        if prox_status==False: 
            print('error getting proxy, quitting')
            sys.exit() 

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

def extract_json_objects(text, decoder=JSONDecoder()):
    """Find JSON objects in text, and yield the decoded JSON data

    Does not attempt to look for JSON arrays, text, or other JSON types outside
    of a parent JSON object.

    """
    pos = 0
    while True:
        match = text.find('{', pos)
        if match == -1:
            break
        try:
            result, index = decoder.raw_decode(text[match:])
            yield result
            pos = match + index
        except ValueError:
            pos = match + 1

def selenium_get_cookies(site_url,timeout, sleep_time):
    from selenium import webdriver
    import time 
    import json 
    import pandas as pd
    start_time = time.time()
    # timeout=30    # cat='Sold'    # prop_id='131116802'    # site_url='https://www.realestate.com.au/sold/property-house-nsw-cootamundra-131116802'
    prox, proxy_type = workProxy()

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--user-agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36')
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--proxy-server="+ proxy_type +"://" + prox)
    chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--window-size=1420,1080")
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    prefs = {}
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", prefs)
    browser = webdriver.Chrome(options=chrome_options)

    browser.get(site_url)
    time.sleep(sleep_time)
    webpage = browser.page_source
    #get cookies used for traditional webscraping
    cook_reauid= browser.get_cookie('reauid')['value']
    cook_bm_aksd= browser.get_cookie('bm_aksd')['value']
    browser.quit()

    print("selenium headers s:", time.time() - start_time)

    return webpage, cook_reauid, cook_bm_aksd

def workProxy():
    import requests 
    import sys
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
    scrape_status=False
    site_url='https://www.realestate.com.au/'
    timeout=5
    loopcount=0
    while scrape_status==False: #do until done
        try:
            if loopcount>=10: #could be a proxy issue 
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
                print("10 failures, new IP time")
                loopcount=0
            loopcount+=1 
            session = requests.Session()
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36','Accept-Language': 'en-US,en;q=0.5'}
            r = session.get(site_url,proxies=proxies,headers=headers,timeout=timeout,verify=False,allow_redirects=True,cookies=session.cookies)
            if len(r.text) > 50:
                scrape_status=True
        except Exception as e:
            scrape_status=False
    print("workProxy:", prox, proxy_type )
    return prox, proxy_type 

def getpropScrapeCount(ps_user, ps_pass, ps_host, ps_port, ps_db, **kwargs):
    #queries and returns total number of rows in db 
    try: 
        with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db) as conn:
            with conn.cursor() as cur:
                cur.execute("select count(*) from sc_land.sc_prop_scrape")
                result = cur.fetchone()
        print("prop scrape count:", result[0])
        proxy_count=result[0]
    except Exception as e: 
        print("error on prop scrape count:", e)
    return proxy_count

def webScrape_page(site_url,timeout, sleep_time, cook_reauid, cook_bm_aksd):
    import requests 
    import time 
    scrape_status='False'
    while scrape_status=='False':
        try:
            #get proxy to use
            prox, proxy_type = workProxy()
            time.sleep(sleep_time)
            proxies={}
            for pt in proxy_type.split(';'): #build dict dynamically 
                if pt =='http': proxies.update({pt : pt + '://' + prox})
                elif pt =='https': proxies.update({pt : pt + '://' + prox})
                elif ( pt =='socks4'or pt=='socks5'): proxies.update({'http' : pt + '://' + prox, 'https' : pt + '://' + prox,})
            session = requests.Session()
            headers={
                'user-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'
                ,'cookie' : 'bm_aksd=' + cook_reauid + ';reauid=' + cook_reauid
                }
            r = session.get(site_url,proxies=proxies,headers=headers,timeout=timeout,verify=False,allow_redirects=True)
            if 'detected some activity in your browser that looks suspicious' in r.text: #bot detected?
                scrape_status='Bot'
                webpage=''
            elif 'detected some activity in your browser that looks suspicious' not in r.text: #bot detected?
                scrape_status='True'
                webpage=r.text
        except Exception as e: 
            print(prox, e)
            scrape_status='False'

def parse_request(webpage,write_folder,cat,prop_id):
    all_json = extract_json_objects(webpage)
    longest=0
    _max=''
    long_count=0
    for result in all_json:
        if len(result) > longest: 
            longest=len(result)
            _max=result
        long_count+=1

    #save vales to dir to be safe
    with open(write_folder + "/" + cat + '_' + prop_id + '.html','x') as file: #html
        file.write(webpage)

    with open(write_folder + "/" + cat + '_' + prop_id + '.json','x') as file: #json
        file.write(json.dumps(_max, ensure_ascii=False, sort_keys=True))

    #now we get writable variables 
    #address 
    try:
        _ad_sub=_max['$' + cat + 'ResidentialListing' + prop_id + '.address']['suburb']
    except: 
        _ad_sub=''

    try:
        _ad_post=_max['$' + cat + 'ResidentialListing' + prop_id + '.address']['postcode']
    except: 
        _ad_post=''

    try:
        _ad_state=_max['$' + cat + 'ResidentialListing' + prop_id + '.address']['state']
    except:
        _ad_state=''

    try:
        _ad_full=_max['$' + cat + 'ResidentialListing' + prop_id + '.address.display']['fullAddress']
    except:
        _ad_full=''

    try:
        _ad_short=_max['$' + cat + 'ResidentialListing' + prop_id + '.address.display']['shortAddress']
    except: 
        _ad_short=''

    #land
    try:
        _ad_ls=_max['$' + cat + 'ResidentialListing' + prop_id + '.propertySizes.land']['displayValue']
    except: 
        _ad_ls=''

    try:
        _ad_ls_unit=_max['$' + cat + 'ResidentialListing' + prop_id + '.propertySizes.land.sizeUnit']['displayValue']
    except: 
        _ad_ls_unit=''

    try:    
        _ad_ls_type=_max['$' + cat + 'ResidentialListing' + prop_id + '.propertySizes.preferred']['sizeType']
    except: 
        _ad_ls_type=''

    #geospatial 
    try:
        _ad_lat=_max['$' + cat + 'ResidentialListing' + prop_id + '.address.display.geocode']['latitude']
    except: 
        _ad_lat=''

    try:
        _ad_long=_max['$' + cat + 'ResidentialListing' + prop_id + '.address.display.geocode']['longitude']
    except: 
        _ad_long=''

    #prop atts
    try:
        _ad_bed=_max['$' + cat + 'ResidentialListing' + prop_id + '.generalFeatures.bedrooms']['value']
    except: 
        _ad_bed=''

    try:
        _ad_bath=_max['$' + cat + 'ResidentialListing' + prop_id + '.generalFeatures.bathrooms']['value']
    except: 
        _ad_bath=''

    try:
        _ad_park=_max['$' + cat + 'ResidentialListing' + prop_id + '.generalFeatures.parkingSpaces']['value']
    except: 
        _ad_park=''

    try:
        _ad_type=_max['$' + cat + 'ResidentialListing' + prop_id + '.propertyType']['id']
    except: 
        _ad_type=''

    #price
    try:
        _ad_price=_max['$' + cat + 'ResidentialListing' + prop_id + '.price']['display']
    except: 
        _ad_price=''

    #agency 
    try:
        _ag_name=_max['$' + cat + 'ResidentialListing' + prop_id + '.listingCompany']['name']
    except: 
        _ag_name=''

    try:
        _ag_address=_max['$' + cat + 'ResidentialListing' + prop_id + '.listingCompany.address.display']['fullAddress']
    except: 
        _ag_address=''

    try:
        _ag_id=_max['$' + cat + 'ResidentialListing' + prop_id + '.listingCompany']['id']
    except: 
        _ag_id=''

    try:
        _ag_type=_max['$' + cat + 'ResidentialListing' + prop_id + '.listingCompany']['__typename']
    except: 
        _ag_type=''

    try:
        _ag_ph=_max['$' + cat + 'ResidentialListing' + prop_id + '.listingCompany']['businessPhone']
    except: 
        _ag_ph=''

    #agent 
    try:
        _agn_name=_max[cat + 'ResidentialListing' + prop_id+ '.listers.0']['name']
    except: 
        _agn_name=''

    #agent 
    try:
        _agn_ph=_max['$' + cat + 'ResidentialListing' + prop_id+ '.listers.0.phoneNumber']['display']
    except: 
        _agn_ph=''

    try:
        _agn_id=_max[cat + 'ResidentialListing' + prop_id+ '.listers.0']['id']
    except: 
        _agn_id=''

    try:
        _agn_agent_id=_max[cat + 'ResidentialListing' + prop_id+ '.listers.0']['display']
    except: 
        _agn_agent_id=''

    _result = pd.DataFrame([{
        'ad_sub': _ad_sub
        ,'ad_post': _ad_post
        ,'ad_state': _ad_state
        ,'ad_full': _ad_full
        ,'ad_short': _ad_short
        ,'ad_ls': _ad_ls
        ,'ad_ls_unit': _ad_ls_unit
        ,'ad_ls_type': _ad_ls_type
        ,'ad_geo_lat': _ad_lat
        ,'ad_geo_long': _ad_long
        ,'ad_bed': _ad_bed
        ,'ad_bath': _ad_bath
        ,'ad_park': _ad_park
        ,'ad_type': _ad_type
        ,'ad_price': _ad_price
        ,'ag_name': _ag_name
        ,'ag_address': _ag_address
        ,'ag_id': _ag_id
        ,'ag_type': _ag_type
        ,'ag_ph': _ag_ph
        ,'agn_name': _agn_name
        ,'agn_ph': _agn_ph
        ,'agn_id': _agn_id
        ,'agn_agent_id': _agn_agent_id
    }])
    _result.to_csv(write_folder + "/test.csv")
    return _result 

def scrape_pages(sql_start, sql_size):
    _start_time=time.time()
    prop_scrape_list = db_import.getPropScrape(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , sql_start=sql_start
        , sql_size=sql_size
        )
    cook_reauid, cook_bm_aksd=''
    _combined=pd.DataFrame()
    for index, row in prop_scrape_list.iterrows(): #dont judge me
        _result=''
        if os.path.exists("/opt/airflow/logs/XML_save_folder/scrape_saves/" + row['state'] + "_" + str(sql_start) + "_" + str(sql_size))==False: #save folder exists?
            os.mkdir("/opt/airflow/logs/XML_save_folder/scrape_saves/" + row['state'] + "_" + str(sql_start) + "_" + str(sql_size)) #make folder
        #check if cookie is still active 
        if cook_reauid=='' and cook_bm_aksd=='':
            webpage, cook_reauid, cook_bm_aksd = selenium_get_cookies(
                site_url=row['url']
                , timeout=30
                , sleep_time=random.randint(1,9)
                )
        elif cook_reauid !='' and cook_bm_aksd !='': #cookies exist, regular scrape
            webpage = webScrape_page(
                site_url=row['url']
                , timeout=30
                , sleep_time=random.randint(1,9)
                , cook_bm_aksd=cook_bm_aksd
                , cook_reauid=cook_reauid
                )
        #now we parse the webpage
        if webpage != 'Bot': #actually returned something 
            _result = parse_request(
                webpage=webpage
                ,write_folder="/opt/airflow/logs/XML_save_folder/scrape_saves/" + row['state'] + "_" + str(sql_start) + "_" + str(sql_size)
                ,cat=row['filetype']
                ,prop_id=str(row['prop_id'])
                )
        elif webpage == 'Bot': #write something here so i know it failed
            _result = pd.DataFrame([{
                'ad_sub': 'Bot'
                ,'ad_post': 'Bot'
                ,'ad_state': 'Bot'
                ,'ad_full': 'Bot'
                ,'ad_short': 'Bot'
                ,'ad_ls': 'Bot'
                ,'ad_ls_unit': 'Bot'
                ,'ad_ls_type': 'Bot'
                ,'ad_geo_lat': 'Bot'
                ,'ad_geo_long': 'Bot'
                ,'ad_bed': 'Bot'
                ,'ad_bath': 'Bot'
                ,'ad_park': 'Bot'
                ,'ad_type': 'Bot'
                ,'ad_price': 'Bot'
                ,'ag_name': 'Bot'
                ,'ag_address': 'Bot'
                ,'ag_id': 'Bot'
                ,'ag_type': 'Bot'
                ,'ag_ph': 'Bot'
                ,'agn_name': 'Bot'
                ,'agn_ph': 'Bot'
                ,'agn_id': 'Bot'
                ,'agn_agent_id': 'Bot'
            }])
        #compile
        _combined = _combined.append(_result, ignore_index=True) #sue me
    #all links checked, now we write
    _combined.to_csv("/opt/airflow/logs/XML_save_folder/scrape_saves/combined" + str(sql_start) + "_" + str(sql_size) + ".csv")
    print("length:", str(sql_size), "-runtime:", time.time() - _start_time, "-avg time:", str( (time.time() - _start_time)/sql_size) )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-mod') #module
    parser.add_argument('-st') #sql limit
    parser.add_argument('-si') #sql offset
    args = vars(parser.parse_args())
    if args['mod'] =='scrape_pages': #args['mod'] =='openproxy':
        print("running scrape_pages")
        scrape_pages(args['st'], args['si'])
        


    