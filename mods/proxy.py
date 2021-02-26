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
    l_dt_val=['hour', 'minute', 'second'] #only get < 24h pages 
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
    for index, row in check_proxy_list.iterrows(): #dont judge me 
        status= error=''
        status, error = testProxy_requests(proxy=row['proxy'], proxy_type=row['proxy_type'],timeout=5, my_ip=myIP)
        l_proxy.append(row['proxy'])
        l_status.append(status)
        l_error.append(error)

    check_proxy_list = pd.DataFrame(
            np.column_stack([l_proxy, l_status,l_error]), 
            columns=['proxy','status','error'])

    with pd.option_context('display.max_rows', len(check_proxy_list), 'display.max_columns', None):  # more options can be specified also
        print(check_proxy_list[['proxy','status','error']])
    
    print(
        str(sql_size)
        ,"proxies checked, -worked:", str(check_proxy_list[check_proxy_list['error'].isnull()].shape[0])
        , "-failed:", str(check_proxy_list[(check_proxy_list['error'] == True)].shape[0])
        )
    #now we write results 
    db_import.updateProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , proxy_list = check_proxy_list[(check_proxy_list['error'] == True)]
        , value='broken'
        )
    db_import.updateProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , proxy_list = check_proxy_list[check_proxy_list['error'].isnull()]
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
    from fake_useragent import UserAgent
    ua = UserAgent()
    headers={ 'User-Agent': ua.random  } 
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
        r = requests.get(url, proxies=proxies,headers=headers, timeout=timeout )
        if my_ip !=r.text: #IP masked
            site_url='https://www.realestate.com.au/'
            r = requests.get(site_url, proxies=proxies, headers=headers, timeout=timeout )
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
    
    return np.array([status, error])
    

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
                if len(browser.page_source) > 50: 
                    #returned proper front page
                    status=True  #holy shit it actually worked
                else: error = url + '-bot blocked -' + _newIP
                status=True                
            except Exception as e:
                error = url + '-' + str(e) 
        else: error = url + '-no IP mask -' + _newIP
    except Exception as e: 
        error = url + '-' + str(e) 

    browser.quit() 
    return np.array([status, error])

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


    
