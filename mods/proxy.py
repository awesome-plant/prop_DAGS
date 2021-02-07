#gets proxy address for testing 
from sqlalchemy import create_engine
import psycopg2
import os 
import sys
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import db_import as db_import
import requests
import pandas as pd
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
    count=0
    #get proxy pages
    for proxy_page in ListlinkerHref:
        if "FRESH" in proxy_page.text :
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
        for IP in (s_scrape.text).splitlines(): #add to list 
            proxylist.append(IP)
            website.append('openproxy.space')
            dt_.append(datetime.datetime.now())
    #now write to df 
    df_proxy_list = pd.DataFrame(
        np.column_stack([proxylist, website,dt_]), 
        columns=['proxy','website','scrape_dt'])
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

    url='https://proxyscrape.com/free-proxy-list'#'https://api.proxyscrape.com/v2/?request=share&protocol=socks4&timeout=400&country=all&simplified=true'
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
    # count=0
    for IP in (s_scrape.text).splitlines(): #add to list 
            proxylist.append(IP)
            website.append('proxyscrape.com')
            dt_.append(datetime.datetime.now())
    #now write to df 
    df_proxy_list = pd.DataFrame(
        np.column_stack([proxylist, website,dt_]), 
        columns=['proxy','website','scrape_dt'])
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
    for a in table.text.splitlines():
        if '.' in a: 
            a_split= a.split(" ")
            proxylist.append(a_split[0] + ":" + a_split[1])
            website.append('proxynova.com')
            dt_.append(datetime.datetime.now())
    #now write to df 
    df_proxy_list = pd.DataFrame(
        np.column_stack([proxylist, website,dt_]), 
        columns=['proxy','website','scrape_dt'])
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

def getProxy(ps_user, ps_pass, ps_host, ps_port, ps_db, update, **kwargs): 
    status=False
    proxy=''
    try: 
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
        status=True
    except Exception as e: 
        print("error on get next proxy:", e)
    return proxy, status

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
    #now we check they work
    check_proxy_list['status'] = check_proxy_list['proxy'].apply(lambda x: testProxy(proxy=x,timeout=3) )
    with pd.option_context('display.max_rows', len(check_proxy_list), 'display.max_columns', None):  # more options can be specified also
        print(check_proxy_list)
    #now we write results 
    db_import.updateProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , proxy_list = check_proxy_list[check_proxy_list['status']==False]
        , value='broken'
        )
    db_import.updateProxies(
        ps_user="postgres"
        , ps_pass="root"
        , ps_host="172.22.114.65"
        , ps_port="5432"
        , ps_db="scrape_db"
        , proxy_list = check_proxy_list[check_proxy_list['status']==True]
        , value='works'
        )

def testProxy(proxy, timeout, **kwargs):
    # def here returns proxy, confirmed with different whatismyip return 
    #return true when dif
    result=False
    try:
        url='https://ident.me/'
        q=requests.get(url)
        _actualIP=q.text
        _newIP=_actualIP
        headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
        proxies= { 'http': 'http://' + proxy, 'https': 'https://' + proxy } 
        try:
            r = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
            _newIP = r.text
            if _actualIP !=_newIP: #IP masked
                try: 
                    site_url='https://www.realestate.com.au/'
                    r = requests.get(site_url, headers=headers, proxies=proxies, timeout=timeout)
                    # print("IP:", proxy, "-capable of scraping:",site_url)
                    result=True
                except:
                    pass
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
    if args['mod'] =='openproxy': #args['mod'] =='openproxy':
        print("running openproxy")
        getProxy_openproxy()
    elif args['mod'] =='proxyscrape':
        print("running proxyscrape")
        getProxy_proxyscrape()
    elif args['mod'] =='proxy_list':
        print("running proxy_list")
        getProxy_proxy_list()
    elif args['mod'] =='proxynova':
        print("running proxynova")
        getProxy_proxynova()
    elif args['mod'] =='check_Proxy':
        print("checking proxies")
        checkProxy(args['st'], args['si'])
            # sys.argv[2], sys.argv[3])

    
