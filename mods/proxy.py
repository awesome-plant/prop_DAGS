#gets proxy address for testing 
from sqlalchemy import create_engine
import psycopg2
import sys

def test(): 
    print('hello world')

def getProxy_openproxy():
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

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_prefs = {}
    chrome_options.experimental_options["prefs"] = chrome_prefs
    chrome_prefs["profile.default_content_settings"] = {"images": 2}

    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    url='https://openproxy.space/list'
    browser = webdriver.Chrome(options=chrome_options)
    browser.get(url)
    browser.execute_script("window.scrollTo(0,document.body.scrollHeight)")
    #get subpage urls 
    browser.implicitly_wait(10)
    time.sleep(5)
    ListlinkerHref = browser.find_elements_by_xpath("//*[@href]")
    proxylist=[] #stores IPs 
    webpage=[] #
    proxy_list=[] #stores IP pages 
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
            webpage.append(proxy_page)

    print("done scraping")
    #now write to df 
    df_proxy_list = pd.DataFrame(
        np.column_stack([proxylist, webpage]), 
        columns=['proxy','webpage'])
    print(df_proxy_list.head())

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
        # proxy={ 
        #     'http' : 'http://' + result[0]
        #     ,'https' : 'https://' + result[0]
        # }
        status=True
    except Exception as e: 
        print("error on get next proxy:", e)
    return proxy, status

if __name__ == '__main__':
    if sys.argv[1] =='openproxy':
        print("this one")