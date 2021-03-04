import os
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import db_import as db_import
import proxy as proxy 
import argparse #add flags here

def site_ScrapeParentURL():  
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
                root = etree.fromstring(r.content)
                body=root.xpath('//ns:Contents',namespaces={'ns':"http://s3.amazonaws.com/doc/2006-03-01/"})
            elif len(r.text) < 50: 
                print("bot blocked IP:",proxy, "lc:", str(loopcount))
                headers={ 'User-Agent': proxy.getHeader(random.randint(0,249))  } 
        except Exception as e:
            print("prox:", str(proxies), "lc:",str(loopcount), "-error:", str(e) )
            scrape_status=False

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

def site_ScrapeChildUrl(sql_start, sql_size): 
    from lxml import etree
    import pandas as pd
    import datetime 
    import numpy as np
    import random
    import requests 
    import time 
    import gzip
    import shutil 
    #passes value to db 
    child_pages_list = db_import.getChildPages(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , sql_start=sql_start
        , sql_size=sql_size
        )
    # myIP=db_import.getCurrentIP( 
        #     ps_user="postgres"
        #     , ps_pass="root"
        #     , ps_host="172.22.114.65"
        #     , ps_port="5432"
        #     , ps_db="scrape_db"
        # )

    #get proxy to use 
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

    # print(child_pages_list)
    #iterate to get each link 
    for index, row in child_pages_list.iterrows(): #dont judge me 
        print("getting: ",index, row['s_filename'])
        time.sleep(2) #forced sleep just in case 
        #not using useragent due to throughput issues 
        headers={ 'User-Agent': proxy.getHeader(random.randint(0,249))  } 
        
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
                # s_filename, s_fileid
                r = requests.get(site_url + row['s_filename'], proxies=proxies,headers=headers, timeout=timeout,verify=False, allow_redirects=True)
                scrape_status=True
            except Exception as e:
                print("prox:", str(proxies), "lc:",str(loopcount), "-error:", str(e) )
                scrape_status=False

        open(row['s_filename'], 'wb').write(r.content)
        with gzip.open(row['s_filename'], 'rb') as f_in:
            with open(row['s_filename'][:-3], 'wb') as f_out: 
                shutil.copyfileobj(f_in, f_out)
        tree = etree.parse(row['s_filename'][:-3])
 
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

        XML_gz_Dataset = pd.DataFrame(
            np.column_stack([list_lastmod, list_url, list_proptype, list_state, list_suburb, list_propid]), 
            columns=['lastmod', 'url', 'proptype', 'state', 'suburb', 'prop_id'])

        XML_gz_Dataset['lastmod']=pd.to_datetime(XML_gz_Dataset['lastmod'])
        # print("total xml time:", time.time() - _time)

        XML_gz_Dataset['s_fileid']=row['s_fileid']
        XML_gz_Dataset['scrape_dt']=(datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
        XML_gz_Dataset['lastmod']=pd.to_datetime(XML_gz_Dataset['lastmod'])
        XML_gz_Dataset['external_ip']=prox

        #time to insert  
        db_import.insertData(
            ps_user="postgres"
            , ps_pass="root"
            , ps_host="172.22.114.65"
            , ps_port="5432"
            , ps_db="scrape_db"
            , table='sc_property_links'
            , df_insert=XML_gz_Dataset
            )
        print("inserted:",row['s_filename'])
    

        # status, error, req_time = getChild_XML(s_filename=row['s_filename'], s_fileid=row['s_fileid'],timeout=5, my_ip=myIP)
            # l_proxy.append(row['proxy'])
            # l_status.append(status)
            # l_error.append(error)
            # l_req_time.append(req_time)
    
    print("done with all of them")
# def getChild_XML(s_filename, s_fileid, timeout, my_ip):

def site_ScrapePageUrl(sql_start, sql_size): 
    #now for the moment we've aaaaall been waiting for baby
    from lxml import etree
    import pandas as pd
    import datetime 
    import numpy as np
    import random
    import requests 
    import time 
    #passes value to db 
    scrape_pages_list = db_import.getScrapePages(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , sql_start=sql_start
        , sql_size=sql_size
        )
    
    for index, row in scrape_pages_list.iterrows():
        


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-mod') #module
    parser.add_argument('-st') #sql limit
    parser.add_argument('-si') #sql offset
    args = vars(parser.parse_args())
    if args['mod'] =='scrape_parent': #args['mod'] =='openproxy':
        print("running scrape_url")
        site_ScrapeParentURL()
    elif args['mod'] =='scrape_child':
        print("running child_url")
        site_ScrapeChildUrl(args['st'], args['si'])