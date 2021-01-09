#gets proxy address for testing 
import time 
import requests 
from fp.fp import FreeProxy
import random

def getProxy(): 
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