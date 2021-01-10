#gets proxy address for testing 
from sqlalchemy import create_engine
import psycopg2

def getProxy(ps_user, ps_pass, ps_host, ps_port, ps_db, update, **kwargs): 
    status=False
    proxy=''
    try: 
        with psycopg2.connect(user=ps_user,password=ps_pass,host=ps_host,port=ps_port,database=ps_db) as conn:
            with conn.cursor() as cur:
                cur.execute("select proxy from sc_land.SC_PROXY_RAW where status ='ready' order by table_id limit 1")
                result = cur.fetchone()
                if update==True:
                    cur.execute("update sc_land.SC_PROXY_RAW set status = 'used' where proxy = %(proxy)s",
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

def oldDNU():
    # def here returns proxy, confirmed with different whatismyip return 
    url='https://ident.me/'
    # ua = UserAgent()
    q=requests.get(url)
    _actualIP=q.text
    _newIP=''
    _getIP_time=time.process_time()
    _try=0
    _checkout=False
    time.sleep(random.randint(1,10))
    while _checkout==False: #_newIP != _actualIP :
        #headers = {'User-Agent':str(ua.random)}
        headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
        proxy = FreeProxy(rand=True).get()
        proxies= { 'http': proxy, 'https': proxy } 
        try:
            r = requests.get(url, headers=headers, proxies=proxies)
            _newIP = r.text
            print("realIP is: ", _actualIP, " - proxy IP is:", _newIP, " - attempt no.", str(_try))
        except Exception as e: 
            print('proxy error, sleep 1-10 try again:', e)
            time.sleep(random.randint(1,10))
        if _actualIP !=_newIP:
            _checkout=True
        _try+=1
    print("fin, total time", str( time.process_time() - _getIP_time ) )
    print("realIP is: ", _actualIP, " - proxy IP is:", _newIP, " - attempt no.", str(_try))
    return proxies, _newIP