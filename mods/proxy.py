#gets proxy address for testing 
from sqlalchemy import create_engine
import psycopg2
import os 
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import db_import as db_import
import requests
import pandas as pd
import numpy as np
import random 
import argparse #add flags here
# import mods.proxy as proxy
# sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
# import db_import as db_import #local file

def getProxy_openproxy():
    from selenium.webdriver.chrome.options import Options
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver import ActionChains
    import time 
    import datetime 
    import numpy as np
    import pandas as pd 
    import re 
    #docker script
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_prefs = {}
    chrome_options.experimental_options["prefs"] = chrome_prefs
    chrome_prefs["profile.default_content_settings"] = {"images": 2}

    # headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    url='https://openproxy.space/list'
    browser = webdriver.Chrome(options=chrome_options)
    browser.get(url)
    browser.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    #get subpage urls 
    browser.implicitly_wait(10)
    time.sleep(5)
    ListlinkerHref = browser.find_elements_by_xpath("//*[@href]")
    proxylist=[] #stores IPs 
    website=[] #
    proxy_list=[] #stores IP pages 
    dt_=[]
    IP_Type=[]
    count=0
    #get proxy pages
    l_dt_val=['week','day','hour', 'minute', 'second'] #only get < 24h pages 
    for proxy_page in ListlinkerHref:
        if len(proxy_page.text) > 0: 
            for a in (proxy_page.text).splitlines():
                if any(val in a for val in l_dt_val):
                    print(count, proxy_page.get_attribute("href"))
                    proxy_list.append(proxy_page.get_attribute("href"))
        count+=1
    #now we get the proxies themselves
    for proxy_page in proxy_list:
        print("scraping page:", proxy_page)
        browser.get(proxy_page)
        browser.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        #get subpage urls 
        browser.implicitly_wait(10)
        time.sleep(5)
        s_scrape = browser.find_element_by_css_selector("textarea") 
        #get proxy type
        nextline=False
        types=''
        for line in (browser.find_element_by_class_name("pa").text).splitlines():
            if nextline==True: 
                types=line
                nextline=False
            elif line=='Protocols': 
                nextline=True
        print(types, proxy_page)
        ip_types=';'.join(re.findall('[A-Z][^A-Z]*', types) )

        for IP in (s_scrape.text).splitlines(): #add to list 
            proxylist.append(IP)
            website.append('openproxy.space')
            dt_.append(datetime.datetime.now())
            IP_Type.append(ip_types.lower())
    #now write to df 
    df_proxy_list = pd.DataFrame(
        np.column_stack([proxylist, website,dt_,IP_Type]), 
        columns=['proxy','website','scrape_dt','proxy_type'])
    browser.quit() 
    print("done scraping, now writing")
    db_import.saveProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , update='proxy-list.download'
        , df_proxy_list=df_proxy_list
        )

def getProxy_proxyscrape():
    from selenium.webdriver.chrome.options import Options
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver import ActionChains
    import time 
    import datetime 
    import numpy as np
    import pandas as pd 

    url='https://proxyscrape.com/free-proxy-list'
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_prefs = {}
    chrome_options.experimental_options["prefs"] = chrome_prefs
    chrome_prefs["profile.default_content_settings"] = {"images": 2}

    browser = webdriver.Chrome(options=chrome_options)
    browser.get(url)
    #get subpage urls 
    browser.implicitly_wait(10)
    time.sleep(5)
    #close overlay if exists 
    if len(browser.find_elements_by_xpath("//iframe[@class='HB-Takeover french-rose']")) >0: #overlay active
        print("clearing active overlay")
        time.sleep(15)
        browser.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        browser.switch_to_frame(browser.find_element_by_xpath("//iframe[@class='HB-Takeover french-rose']"))
        time.sleep(15)
        click_stat=False
        while click_stat ==False:
            try:
                browser.execute_script("arguments[0].click();", browser.find_element_by_class_name("icon-close"))
                click_stat=True
            except Exception as e: 
                print("error sleep 10, trying again:", str(e))
                time.sleep(10)
        print("overlay cleared")
    #get to proxy page
    browser.switch_to.default_content() #swap from iframe window
    # browser.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    browser.maximize_window()
    # https://stackoverflow.com/questions/40485157/how-to-move-range-input-using-selenium-in-python
    en =  browser.find_element_by_id("socks4timeoutslide")
    move = ActionChains(browser)
    move.click_and_hold(en).move_by_offset(-95, 0).release().perform()
    button = browser.find_element_by_id("sharesocks4") #browser.find_elements_by_class_name('downloadbtn')[1]
    button.click()
    #load new page
    browser.implicitly_wait(10)
    time.sleep(5)
    s_scrape = browser.find_element_by_css_selector("textarea") 
    proxylist=[] #stores IPs 
    website=[] #
    dt_=[]
    IP_Type=[]
    # count=0
    for IP in (s_scrape.text).splitlines(): #add to list 
            proxylist.append(IP)
            website.append('proxyscrape.com')
            dt_.append(datetime.datetime.now())
            IP_Type.append('socks4')
    #now write to df 
    df_proxy_list = pd.DataFrame(
        np.column_stack([proxylist, website,dt_,IP_Type]), 
        columns=['proxy','website','scrape_dt','proxy_type'])
    browser.quit() 
    print("done scraping, now writing")
    db_import.saveProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , update='proxy-list.download'
        , df_proxy_list=df_proxy_list
        )

def getProxy_proxy_list():
    from selenium.webdriver.chrome.options import Options
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver import ActionChains
    import time 
    import datetime 
    import numpy as np
    import pandas as pd 

    url='https://www.proxy-list.download/SOCKS4'
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_prefs = {}
    chrome_options.experimental_options["prefs"] = chrome_prefs
    chrome_prefs["profile.default_content_settings"] = {"images": 2}
    chrome_options.add_argument("--disable-popup-blocking")
    browser = webdriver.Chrome(options=chrome_options)
    browser.get(url)
    #get subpage urls 
    browser.implicitly_wait(10)
    time.sleep(5)
    s_scrape = browser.find_element_by_id("downloadbtn") #browser.find_element_by_css_selector("textarea") 
    s_scrape.click() #download as txt file 
    time.sleep(15) #allow file to download
    df_proxy_list = pd.read_csv('Proxy List.txt',sep="\t", names=['proxy']) #import to df 
    df_proxy_list['website']='proxy-list.download'
    df_proxy_list['scrape_dt']=datetime.datetime.now()
    df_proxy_list['proxy_type']='socks4'
    # print(df_proxy_list.head())
    browser.quit() 
    print("done scraping, now writing")
    db_import.saveProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , update='proxy-list.download'
        , df_proxy_list=df_proxy_list
        )

def getProxy_proxynova():
    from selenium.webdriver.chrome.options import Options
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver import ActionChains
    import time 
    import datetime 
    import numpy as np
    import pandas as pd 

    url='https://www.proxynova.com/proxy-server-list/anonymous-proxies/'

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_prefs = {}
    chrome_options.experimental_options["prefs"] = chrome_prefs
    chrome_prefs["profile.default_content_settings"] = {"images": 2}
    chrome_options.add_argument("--disable-popup-blocking")
    browser = webdriver.Chrome(options=chrome_options)
    browser.get(url)
    table = browser.find_element_by_id("tbl_proxy_list")
    proxylist=[]
    website=[]
    dt_=[]
    IP_Type=[]
    for a in table.text.splitlines():
        if '.' in a: 
            a_split= a.split(" ")
            proxylist.append(a_split[0] + ":" + a_split[1])
            website.append('proxynova.com')
            dt_.append(datetime.datetime.now())
            IP_Type.append('http')
    #now write to df 
    df_proxy_list = pd.DataFrame(
        np.column_stack([proxylist, website,dt_,IP_Type]), 
        columns=['proxy','website','scrape_dt','proxy_type'])
    browser.quit() 
    print("done scraping, now writing")
    db_import.saveProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , update='proxy-list.download'
        , df_proxy_list=df_proxy_list
        )

def getProxy_proxyscan():
    #docker script
    from selenium.webdriver.chrome.options import Options
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver import ActionChains
    import time 
    import numpy as np
    import pandas as pd
    import datetime  
    import lxml

    url='https://www.proxyscan.io/'
    ping=300

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_prefs = {}
    chrome_options.experimental_options["prefs"] = chrome_prefs
    chrome_prefs["profile.default_content_settings"] = {"images": 2}
    browser = webdriver.Chrome(options=chrome_options)

    browser.get(url)
    #click buttons 
    browser.find_element_by_id("pingText").send_keys(str(ping))
    ActionChains(browser).move_to_element(browser.find_element_by_id("anonymous")).click().perform()
    ActionChains(browser).move_to_element(browser.find_element_by_id("elite")).click().perform()
    #scroll to bottom of page
    sc_stat=False
    ph=0 #page height
    while sc_stat==False:
        if browser.execute_script("return window.pageYOffset") <= browser.execute_script("return document.documentElement.scrollHeight"): 
            ph = browser.execute_script("return window.pageYOffset")
            browser.execute_script("window.scrollTo(0,document.documentElement.scrollHeight)")
            time.sleep(1)
            if ph == browser.execute_script("return window.pageYOffset"): 
                sc_stat=True 
    #get table 
    table_MN = pd.read_html(browser.page_source)
    filtered = table_MN[0][table_MN[0]['Anonymity']!='Transparent']
    df_proxy_list=pd.DataFrame()
    df_proxy_list['proxy'] = filtered['Ip Address'].astype(str) + ":" + filtered['Port'].astype(str)
    df_proxy_list['website']='proxyscan'
    df_proxy_list['scrape_dt']=datetime.datetime.now()
    df_proxy_list['proxy_type']=filtered['Type'].replace(',',';', regex=True).str.lower()

    browser.quit() 
    print("done scraping, now writing")
    db_import.saveProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , update='proxyscan'
        , df_proxy_list=df_proxy_list
        )

def getProxy(ps_user, ps_pass, ps_host, ps_port, ps_db, update, **kwargs): 
    # status=False
    proxy=''
    # try: 
    with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db) as conn:
        with conn.cursor() as cur:
            cur.execute("select proxy from sc_land.sc_proxy_raw where status ='ready' order by table_id limit 1")
            result = cur.fetchone()
            if update==True:
                cur.execute("update sc_land.sc_proxy_raw set status = 'used' where proxy = %(proxy)s",
                    {
                        'proxy': result[0]
                    }
                )
                conn.commit()
    print("proxy used is:", result[0])
    proxy=result[0]
    # status=True
    # except Exception as e: 
    #     print("error on get next proxy:", e)
    return proxy #, status

def getProxyCount(ps_user, ps_pass, ps_host, ps_port, ps_db, **kwargs):
    #queries and returns total number of rows in db 
    # amt = proxy.getProxyCount(ps_user="postgres", ps_pass="root", ps_host="172.22.114.65", ps_port="5432", ps_db="scrape_db")
    try: 
        with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db) as conn:
            with conn.cursor() as cur:
                cur.execute("select count(*) from sc_land.sc_proxy_raw")
                result = cur.fetchone()
        print("proxy count:", result[0])
        proxy_count=result[0]
    except Exception as e: 
        print("error on get next proxy:", e)
    return proxy_count

def refreshIP(ps_user, ps_pass, ps_host, ps_port, ps_db):
    # gets current IP and writes to db
    #get current IP 
    import re #used to strip html 
    from selenium.webdriver.chrome.options import Options
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver import ActionChains
    url='https://ident.me/'
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_prefs = {}
    chrome_options.experimental_options["prefs"] = chrome_prefs
    chrome_prefs["profile.default_content_settings"] = {"images": 2}
    chrome_options.add_argument("--disable-popup-blocking")
    browser = webdriver.Chrome(options=chrome_options)
    browser.get(url)
    my_ip = re.sub('<[^<]+?>', '', browser.page_source) #strip html 
    browser.quit()
    print("ip is:", my_ip)

    if len(my_ip) > 0: #write to db
        with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db) as conn:
            with conn.cursor() as cur:
                cur.execute("truncate sc_land.sc_cur_ip")
                conn.commit()
                cur.execute("insert into sc_land.sc_cur_ip (cur_ip) VALUES(%(my_ip)s)", { 'my_ip': my_ip })
                conn.commit()

def checkProxy(sql_start, sql_size):
    check_proxy_list = db_import.getProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , sql_start=sql_start
        , sql_size=sql_size
        )
    myIP=db_import.getCurrentIP( 
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
    )
    print("current IP is:", str(myIP))
    #now we check they work
    l_proxy=[]
    l_status=[]
    l_error=[]
    l_req_time=[]
    for index, row in check_proxy_list.iterrows(): #dont judge me 
        status= error=''
        status, error, req_time = testProxy_requests(proxy=row['proxy'], proxy_type=row['proxy_type'],timeout=5, my_ip=myIP)
        l_proxy.append(row['proxy'])
        l_status.append(status)
        l_error.append(error)
        l_req_time.append(req_time)

    check_proxy_list = pd.DataFrame(
        np.column_stack([l_proxy, l_status,l_error,l_req_time]), 
        columns=['proxy','status','error','req_time'])

    with pd.option_context('display.max_rows', len(check_proxy_list), 'display.max_columns', None):  # more options can be specified also
        print(check_proxy_list[['proxy','status','error','req_time']])
    
    print(
        str(sql_size)
        ,"proxies checked, -worked:", str(check_proxy_list[(check_proxy_list['status'] == 'True')].shape[0])
    , "-failed:", str(check_proxy_list[(check_proxy_list['status'] == 'False')].shape[0])
    )
    #now we write results 
    db_import.updateProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , proxy_list = check_proxy_list[(check_proxy_list['status'] == 'False')]
        , value='broken'
        )
    db_import.updateProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , proxy_list = check_proxy_list[(check_proxy_list['status'] == 'True')]
        , value='ready'
        )

def testProxy_selenium(proxy, timeout, my_ip, **kwargs):
    # def here returns proxy, confirmed with different whatismyip return 
    #return true when dif
    from selenium.webdriver.chrome.options import Options
    from selenium import webdriver
    from selenium.webdriver import ActionChains
    from selenium.webdriver.common.proxy import Proxy, ProxyType
    status=False
    error=None

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--proxy-server=http://" + proxy)
    chrome_options.add_argument('--blink-settings=imagesEnabled=false')
    chrome_prefs = {}
    chrome_options.experimental_options["prefs"] = chrome_prefs
    # chrome_prefs["profile.default_content_settings"] = {"images": 2}
    browser = webdriver.Chrome(options=chrome_options)

    prox = Proxy()
    prox.proxy_type = ProxyType.MANUAL
    prox.http_proxy = proxy
    prox.socks_proxy = proxy
    prox.ssl_proxy = proxy
    url='http://ident.me/'

    try:
        browser.set_page_load_timeout(timeout)
        browser.get(url)
        _newIP = browser.find_element_by_tag_name("body").text
        if my_ip !=_newIP: #IP masked
            try: 
                site_url='https://www.realestate.com.au/'
                browser.get(site_url)
                # if len(browser.page_source) > 50: 
                #     #returned proper front page
                #     status=True  #holy shit it actually worked
                # else: error = url + '-bot blocked -' + _newIP
                status=True                
            except Exception as e:
                error = url + '-' + str(e) 
        else: error = url + '-no IP mask -' + _newIP
    except Exception as e: 
        error = url + '-' + str(e) 

    browser.quit() 
    return np.array([status, error])

def testProxy_requests(proxy, proxy_type, timeout, my_ip, **kwargs):
    import requests 
    import time
    import math
    time.sleep(2) #forced sleep just in case 
    #not using useragent due to throughput issues 
    headers={ 'User-Agent': getHeader(random.randint(1,250))  } 
    proxies={}

    for pt in proxy_type.split(';'): #build dict dynamically 
        if pt =='http': proxies.update({pt : pt + '://' + proxy})
        elif pt =='https': proxies.update({pt : pt + '://' + proxy})
        elif ( pt =='socks4'or pt=='socks5'): proxies.update({'http' : pt + '://' + proxy, 'https' : pt + '://' + proxy,})

    url='https://ident.me/'
    error=''
    status=False
    # loopcount=0
    # scrape=False
    try:
        start_time = time.time()
        r = requests.get(url, proxies=proxies,headers=headers, timeout=timeout,verify=False )
        if my_ip !=r.text: #IP masked
            site_url='https://www.realestate.com.au/'
            r = requests.get(site_url, proxies=proxies, headers=headers, timeout=timeout,verify=False )
            # while scrape==False:
                #     if len(r.text) > 50: 
                #         headers={ 'User-Agent': ua.random  } 
                #         r = requests.get(site_url, proxies=proxies, headers=headers, timeout=timeout )
                #         status=True  #holy shit it actually worked
                #         loopcount+=1
                #         scrape=True
                #     elif loopcount > 5: 
                #         error = site_url + '-bot blocked -' + r.text
            status=True                
        else: error = url + '-no IP mask -' + r.text
    except Exception as e: 
        error = url + '-' + str(e) 
    
    return np.array([status, error, math.ceil(time.time() - start_time)])

def proxyerror(proxy, timeout, **kwargs):
    # def here returns proxy, confirmed with different whatismyip return 
    #return true when dif
    result=''
    try:
        url='https://ident.me/'
        q=requests.get(url)
        _actualIP=q.text
        _newIP=_actualIP
        headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
        proxies= { 'http': 'http://' + proxy}#, 'https': 'https://' + proxy } 
        try:
            r = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
            _newIP = r.text
            if _actualIP !=_newIP: #IP masked
                try: 
                    site_url='https://www.realestate.com.au/'
                    r = requests.get(site_url, headers=headers, proxies=proxies, timeout=timeout)
                    # print("IP:", proxy, "-capable of scraping:",site_url)
                    result=True
                except Exception as e:
                    result='error: ' + str(e)
        except: 
            pass
    except: 
        pass
    # if result==True: print("IP:", proxy, "-capable of scraping:",site_url)
    return result  

def getHeader(value):
    # returns header value, cant use db since too many db calls cause issue
    #cant use existing lib for same issue 
    headers=[
        'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:23.0) Gecko/20131011 Firefox/23.0',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/41.0.2227.1 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/41.0.2227.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/41.0.2227.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/41.0.2226.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/41.0.2225.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/41.0.2225.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/41.0.2224.3 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML like Gecko) Chrome/40.0.2214.93 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2062.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2049.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 4.0; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/37.0.2049.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/36.0.1985.67 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/36.0.1985.67 Safari/537.36',
        'Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML like Gecko) Chrome/36.0.1985.125 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML like Gecko) Chrome/36.0.1944.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/35.0.3319.102 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/35.0.2309.372 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/35.0.2117.157 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML like Gecko) Chrome/35.0.1916.47 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/34.0.1866.237 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/34.0.1847.137 Safari/4E423F',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/34.0.1847.116 Safari/537.36 Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML like Gecko) Version/4.0.4 Mobile/7B334b Safari/531.21.10',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/33.0.1750.517 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/32.0.1667.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML like Gecko) Chrome/32.0.1664.3 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/537.36 (KHTML like Gecko) Chrome/32.0.1664.3 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/31.0.1650.16 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/31.0.1623.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/30.0.1599.17 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/29.0.1547.62 Safari/537.36',
        'Mozilla/5.0 (X11; CrOS i686 4319.74.0) AppleWebKit/537.36 (KHTML like Gecko) Chrome/29.0.1547.57 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/29.0.1547.2 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/28.0.1468.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML like Gecko) Chrome/28.0.1467.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML like Gecko) Chrome/28.0.1464.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/27.0.1500.55 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML like Gecko) Chrome/27.0.1453.93 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.2; rv:22.0) Gecko/20130405 Firefox/22.0',
        'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML like Gecko) Chrome/27.0.1453.90 Safari/537.36',
        'Mozilla/5.0 (X11; NetBSD) AppleWebKit/537.36 (KHTML like Gecko) Chrome/27.0.1453.116 Safari/537.36',
        'Mozilla/5.0 (X11; CrOS i686 3912.101.0) AppleWebKit/537.36 (KHTML like Gecko) Chrome/27.0.1453.116 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML like Gecko) Chrome/24.0.1312.60 Safari/537.17',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML like Gecko) Chrome/24.0.1309.0 Safari/537.17',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.15 (KHTML like Gecko) Chrome/24.0.1295.0 Safari/537.15',
        'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.14 (KHTML like Gecko) Chrome/24.0.1292.0 Safari/537.14',
        'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16',
        'Mozilla/5.0 (Windows NT 6.0; rv:2.0) Gecko/20100101 Firefox/4.0 Opera 12.14',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0) Opera 12.14',
        'Opera/12.80 (Windows NT 5.1; U; en) Presto/2.10.289 Version/12.02',
        'Opera/9.80 (Windows NT 6.1; U; es-ES) Presto/2.9.181 Version/12.00',
        'Opera/9.80 (Windows NT 5.1; U; zh-sg) Presto/2.9.181 Version/12.00',
        'Opera/12.0(Windows NT 5.2;U;en)Presto/22.9.168 Version/12.00',
        'Opera/12.0(Windows NT 5.1;U;en)Presto/22.9.168 Version/12.00',
        'Mozilla/5.0 (Windows NT 5.1) Gecko/20100101 Firefox/14.0 Opera/12.0',
        'Opera/9.80 (Windows NT 6.1; WOW64; U; pt) Presto/2.10.229 Version/11.62',
        'Opera/9.80 (Windows NT 6.0; U; pl) Presto/2.10.229 Version/11.62',
        'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52',
        'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; de) Presto/2.9.168 Version/11.52',
        'Opera/9.80 (Windows NT 5.1; U; en) Presto/2.9.168 Version/11.51',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; de) Opera 11.51',
        'Opera/9.80 (X11; Linux x86_64; U; fr) Presto/2.9.168 Version/11.50',
        'Opera/9.80 (X11; Linux i686; U; hu) Presto/2.9.168 Version/11.50',
        'Opera/9.80 (X11; Linux i686; U; ru) Presto/2.8.131 Version/11.11',
        'Opera/9.80 (X11; Linux i686; U; es-ES) Presto/2.8.131 Version/11.11',
        'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/5.0 Opera 11.11',
        'Opera/9.80 (X11; Linux x86_64; U; bg) Presto/2.8.131 Version/11.10',
        'Opera/9.80 (Windows NT 6.0; U; en) Presto/2.8.99 Version/11.10',
        'Opera/9.80 (Windows NT 5.1; U; zh-tw) Presto/2.8.131 Version/11.10',
        'Opera/9.80 (Windows NT 6.1; Opera Tablet/15165; U; en) Presto/2.8.149 Version/11.1',
        'Opera/9.80 (X11; Linux x86_64; U; Ubuntu/10.10 (maverick); pl) Presto/2.7.62 Version/11.01',
        'Opera/9.80 (X11; Linux i686; U; ja) Presto/2.7.62 Version/11.01',
        'Opera/9.80 (X11; Linux i686; U; fr) Presto/2.7.62 Version/11.01',
        'Opera/9.80 (Windows NT 6.1; U; zh-tw) Presto/2.7.62 Version/11.01',
        'Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.7.62 Version/11.01',
        'Opera/9.80 (Windows NT 6.1; U; sv) Presto/2.7.62 Version/11.01',
        'Opera/9.80 (Windows NT 6.1; U; en-US) Presto/2.7.62 Version/11.01',
        'Opera/9.80 (Windows NT 6.1; U; cs) Presto/2.7.62 Version/11.01',
        'Opera/9.80 (Windows NT 6.0; U; pl) Presto/2.7.62 Version/11.01',
        'Opera/9.80 (Windows NT 5.2; U; ru) Presto/2.7.62 Version/11.01',
        'Opera/9.80 (Windows NT 5.1; U;) Presto/2.7.62 Version/11.01',
        'Opera/9.80 (Windows NT 5.1; U; cs) Presto/2.7.62 Version/11.01',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.13) Gecko/20101213 Opera/9.80 (Windows NT 6.1; U; zh-tw) Presto/2.7.62 Version/11.01',
        'Mozilla/5.0 (Windows NT 6.1; U; nl; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6 Opera 11.01',
        'Mozilla/5.0 (Windows NT 6.1; U; de; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6 Opera 11.01',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; de) Opera 11.01',
        'Opera/9.80 (X11; Linux x86_64; U; pl) Presto/2.7.62 Version/11.00',
        'Opera/9.80 (X11; Linux i686; U; it) Presto/2.7.62 Version/11.00',
        'Opera/9.80 (Windows NT 6.1; U; zh-cn) Presto/2.6.37 Version/11.00',
        'Opera/9.80 (Windows NT 6.1; U; pl) Presto/2.7.62 Version/11.00',
        'Opera/9.80 (Windows NT 6.1; U; ko) Presto/2.7.62 Version/11.00',
        'Opera/9.80 (Windows NT 6.1; U; fi) Presto/2.7.62 Version/11.00',
        'Opera/9.80 (Windows NT 6.1; U; en-GB) Presto/2.7.62 Version/11.00',
        'Opera/9.80 (Windows NT 6.1 x64; U; en) Presto/2.7.62 Version/11.00',
        'Opera/9.80 (Windows NT 6.0; U; en) Presto/2.7.39 Version/11.00',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1',
        'Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0',
        'Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20130401 Firefox/31.0',
        'Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20120101 Firefox/29.0',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/29.0',
        'Mozilla/5.0 (X11; OpenBSD amd64; rv:28.0) Gecko/20100101 Firefox/28.0',
        'Mozilla/5.0 (X11; Linux x86_64; rv:28.0) Gecko/20100101  Firefox/28.0',
        'Mozilla/5.0 (Windows NT 6.1; rv:27.3) Gecko/20130101 Firefox/27.3',
        'Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:27.0) Gecko/20121011 Firefox/27.0',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:25.0) Gecko/20100101 Firefox/25.0',
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:24.0) Gecko/20100101 Firefox/24.0',
        'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:24.0) Gecko/20100101 Firefox/24.0',
        'Mozilla/5.0 (Windows NT 6.2; rv:22.0) Gecko/20130405 Firefox/23.0',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:22.0) Gecko/20130328 Firefox/22.0',
        'Mozilla/5.0 (Windows NT 6.1; rv:22.0) Gecko/20130405 Firefox/22.0',
        'Mozilla/5.0 (Microsoft Windows NT 6.2.9200.0); rv:22.0) Gecko/20130405 Firefox/22.0',
        'Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:16.0.1) Gecko/20121011 Firefox/21.0.1',
        'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:16.0.1) Gecko/20121011 Firefox/21.0.1',
        'Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:21.0.0) Gecko/20121011 Firefox/21.0.0',
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:21.0) Gecko/20130331 Firefox/21.0',
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:21.0) Gecko/20100101 Firefox/21.0',
        'Mozilla/5.0 (X11; Linux i686; rv:21.0) Gecko/20100101 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20130514 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 6.2; rv:21.0) Gecko/20130326 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130401 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130331 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130330 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20130401 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20130328 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20100101 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 5.1; rv:21.0) Gecko/20130401 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 5.1; rv:21.0) Gecko/20130331 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 5.1; rv:21.0) Gecko/20100101 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 5.0; rv:21.0) Gecko/20100101 Firefox/21.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) Gecko/20100101 Firefox/21.0',
        'Mozilla/5.0 (Windows NT 6.2; Win64; x64;) Gecko/20100101 Firefox/20.0',
        'Mozilla/5.0 (Windows x86; rv:19.0) Gecko/20100101 Firefox/19.0',
        'Mozilla/5.0 (Windows NT 6.1; rv:6.0) Gecko/20100101 Firefox/19.0',
        'Mozilla/5.0 (Windows NT 6.1; rv:14.0) Gecko/20100101 Firefox/18.0.1',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0)  Gecko/20100101 Firefox/18.0',
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:17.0) Gecko/20100101 Firefox/17.0.6',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible MSIE 11 Windows NT 6.3; Trident/7.0;  rv:11.0) like Gecko',
        'Mozilla/5.0 (compatible; MSIE 10.6; Windows NT 6.1; Trident/5.0; InfoPath.2; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 2.0.50727) 3gpp-gba UNTRUSTED/1.0',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 7.0; InfoPath.3; .NET CLR 3.1.40767; Trident/6.0; en-IN)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/5.0)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/4.0; InfoPath.2; SV1; .NET CLR 2.0.50727; WOW64)',
        'Mozilla/5.0 (compatible; MSIE 10.0; Macintosh; Intel Mac OS X 10_7_3; Trident/6.0)',
        'Mozilla/4.0 (Compatible; MSIE 8.0; Windows NT 5.2; Trident/6.0)',
        'Mozilla/4.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/5.0)',
        'Mozilla/1.22 (compatible; MSIE 10.0; Windows 3.1)',
        'Mozilla/5.0 (Windows; U; MSIE 9.0; WIndows NT 9.0; en-US))',
        'Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 7.1; Trident/5.0)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 4.0; InfoPath.3; MS-RTC LM 8; .NET4.0C; .NET4.0E)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; chromeframe/12.0.742.112)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 4.0; Tablet PC 2.0; InfoPath.3; .NET4.0C; .NET4.0E)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; yie8)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET CLR 1.1.4322; .NET4.0C; Tablet PC 2.0)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; FunWebProducts)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; chromeframe/13.0.782.215)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; chromeframe/11.0.696.57)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0) chromeframe/10.0.648.205',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/4.0; GTB7.4; InfoPath.1; SV1; .NET CLR 2.8.52393; WOW64; en-US)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; chromeframe/11.0.696.57)',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/4.0; GTB7.4; InfoPath.3; SV1; .NET CLR 3.1.76908; WOW64; en-US)',
        'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; GTB7.4; InfoPath.2; SV1; .NET CLR 3.3.69573; WOW64; en-US)',
        'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)',
        'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; InfoPath.1; SV1; .NET CLR 3.8.36217; WOW64; en-US)',
        'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; .NET CLR 2.7.58687; SLCC2; Media Center PC 5.0; Zune 3.4; Tablet PC 3.6; InfoPath.3)',
        'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.2; Trident/4.0; Media Center PC 4.0; SLCC1; .NET CLR 3.0.04320)',
        'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 1.1.4322)',
        'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; InfoPath.2; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 2.0.50727)',
        'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
        'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; SLCC1; .NET CLR 1.1.4322)',
        'Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.0; Trident/4.0; InfoPath.1; SV1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 3.0.04506.30)',
        'Mozilla/5.0 (compatible; MSIE 7.0; Windows NT 5.0; Trident/4.0; FBSMTWB; .NET CLR 2.0.34861; .NET CLR 3.0.3746.3218; .NET CLR 3.5.33652; msn OptimizedIE8;ENUS)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.2; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; Media Center PC 6.0; InfoPath.2; MS-RTC LM 8)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; Media Center PC 6.0; InfoPath.2; MS-RTC LM 8',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; Media Center PC 6.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; InfoPath.3; .NET4.0C; .NET4.0E; .NET CLR 3.5.30729; .NET CLR 3.0.30729; MS-RTC LM 8)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; InfoPath.2)',
        'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 3.0)',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML like Gecko) Version/7.0.3 Safari/7046A194A',
        'Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML like Gecko) Version/6.0 Mobile/10A5355d Safari/8536.25',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13+ (KHTML like Gecko) Version/5.1.7 Safari/534.57.2',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML like Gecko) Version/5.1.3 Safari/534.53.10',
        'Mozilla/5.0 (iPad; CPU OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML like Gecko ) Version/5.1 Mobile/9B176 Safari/7534.48.3',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; de-at) AppleWebKit/533.21.1 (KHTML like Gecko) Version/5.0.5 Safari/533.21.1',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; da-dk) AppleWebKit/533.21.1 (KHTML like Gecko) Version/5.0.5 Safari/533.21.1',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; tr-TR) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; ko-KR) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; fr-FR) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; cs-CZ) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Windows; U; Windows NT 6.0; ja-JP) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_5_8; zh-cn) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_5_8; ja-jp) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; ja-jp) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; zh-cn) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; sv-se) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; ko-kr) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; ja-jp) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; it-it) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; fr-fr) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; es-es) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-us) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-gb) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; de-de) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.4 Safari/533.20.27',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; sv-SE) AppleWebKit/533.19.4 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; ja-JP) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; de-DE) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Windows; U; Windows NT 6.0; hu-HU) AppleWebKit/533.19.4 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Windows; U; Windows NT 6.0; de-DE) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.19.4 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Windows; U; Windows NT 5.1; ja-JP) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Windows; U; Windows NT 5.1; it-IT) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/533.20.25 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; en-us) AppleWebKit/534.16+ (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; fr-ch) AppleWebKit/533.19.4 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_5; de-de) AppleWebKit/534.15+ (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_5; ar) AppleWebKit/533.19.4 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Android 2.2; Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.19.4 (KHTML like Gecko) Version/5.0.3 Safari/533.19.4',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; zh-HK) AppleWebKit/533.18.1 (KHTML like Gecko) Version/5.0.2 Safari/533.18.5',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.19.4 (KHTML like Gecko) Version/5.0.2 Safari/533.18.5',
        'Mozilla/5.0 (Windows; U; Windows NT 6.0; tr-TR) AppleWebKit/533.18.1 (KHTML like Gecko) Version/5.0.2 Safari/533.18.5',
        'Mozilla/5.0 (Windows; U; Windows NT 6.0; nb-NO) AppleWebKit/533.18.1 (KHTML like Gecko) Version/5.0.2 Safari/533.18.5',
        'Mozilla/5.0 (Windows; U; Windows NT 6.0; fr-FR) AppleWebKit/533.18.1 (KHTML like Gecko) Version/5.0.2 Safari/533.18.5',
        'Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-TW) AppleWebKit/533.19.4 (KHTML like Gecko) Version/5.0.2 Safari/533.18.5',
        'Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.18.1 (KHTML like Gecko) Version/5.0.2 Safari/533.18.5',
        'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_8; zh-cn) AppleWebKit/533.18.1 (KHTML like Gecko) Version/5.0.2 Safari/533.18.5',
        ]
    return headers[value]

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-mod') #module
    parser.add_argument('-st') #sql limit
    parser.add_argument('-si') #sql offset
    args = vars(parser.parse_args())
    if args['mod'] =='open_proxy': #args['mod'] =='openproxy':
        print("running open_proxy")
        getProxy_openproxy()
    elif args['mod'] =='proxy_scrape':
        print("running proxy_scrape")
        getProxy_proxyscrape()
    elif args['mod'] =='proxy_list':
        print("running proxy_list")
        getProxy_proxy_list()
    elif args['mod'] =='proxy_nova':
        print("running proxy_nova")
        getProxy_proxynova()
    elif args['mod'] =='proxy_scan':
        print("running proxy_scan")
        getProxy_proxyscan()
    elif args['mod'] =='check_Proxy':
        print("checking proxies")
        checkProxy(args['st'], args['si'])
    elif args['mod']=='refresh_ip':
        print("refreshing IP")
        refreshIP( ps_user="postgres", ps_pass="root", ps_host="172.22.114.65", ps_port="5432", ps_db="scrape_db" )


    
