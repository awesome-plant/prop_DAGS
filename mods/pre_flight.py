import requests
from lxml.html import fromstring
import pandas as pd
import mods.db_import as db_import 
from sqlalchemy import create_engine
import psycopg2

def preFlightProxy(timeout):
    url = 'https://sslproxies.org/'
    response = requests.get(url)
    parser = fromstring(response.text)
    IP=[]
    result=False
    try: 
        for i in parser.xpath('//textarea'): #tbody/tr'):
            # print(i.text.strip())
            # print('---')
            for x in i.text.splitlines():
                if x.strip()[:1].isnumeric() ==True: 
                    IP.append(x.strip())
        df_proxies = pd.DataFrame.from_dict(IP)
        df_proxies=df_proxies.rename(columns={ df_proxies.columns[0]:'proxy'})
        df_proxies['result']=df_proxies.apply(lambda x: testProxy(x['proxy'], 5), axis=1) 
        result=True
    except Exception as e: 
        print("error on preflight getProxy:", e)
    return df_proxies[df_proxies['result']], result

def testProxy(proxy, timeout):
    # def here returns proxy, confirmed with different whatismyip return 
    #return true when dif
    url='https://ident.me/'
    q=requests.get(url)
    _actualIP=q.text
    _newIP=_actualIP
    result=False
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    proxies= { 'http': 'http://' + proxy, 'https': 'https://' + proxy } 
    try:
        r = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
        _newIP = r.text
        # print("realIP is: ", _actualIP, " - proxy IP is:", _newIP)
    except Exception as e: 
        # print('proxy error:', e)
        pass
    if _actualIP !=_newIP:
        result=True
    print("proxy:", proxy, "- result:", result)
    return result

def newProxyList(df_proxies):
    result=False
    try:
        df_proxies['status']='ready'
        #does the following things 
        # 1. move raw -> his 
        # 2. truncate raw 
        # 3. import df to raw 
        #connection details here 
        ps_user="postgres"
        ps_pass="root"
        ps_host="172.22.114.65"
        ps_port="5432"
        ps_db="scrape_db"

        with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db) as conn:
            with conn.cursor() as cur:
                cur.execute("insert into sc_land.SC_PROXY_HIS (proxy, scrape_dt, status) select proxy, now(), status from sc_land.sc_proxy_raw")
                conn.commit()
                cur.execute("truncate table sc_land.sc_proxy_raw")
                conn.commit()
        engine = create_engine('postgresql://' + ps_user + ':' + ps_pass + '@' + ps_host + ':' + ps_port + '/' + ps_db)
        df_proxies[['proxy','status']].to_sql(
            name='sc_proxy_raw'
            ,schema='sc_land'
            ,con=engine
            ,method=db_import.psql_insert_copy
            ,if_exists='append'
            ,index=False
            )
        result=True
    except Exception as e:
        print("error on reflight impProxy:", e)
    return result