
from selenium import webdriver

status=False
error=None
timeout=30
proxy='141.164.56.244:8080'

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--proxy-server=http://" + proxy)
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
browser = webdriver.Chrome(options=chrome_options)

url='http://ident.me/'
browser.set_page_load_timeout(timeout)
browser.get(url)
browser.find_element_by_tag_name("body").text
-------------------
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
status=False
error=None
timeout=30
proxy='141.164.56.244:8080'
userAgent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"

chrome_options = Options()
chrome_options.add_argument("--no-sandbox") 
chrome_options.add_argument("--disable-setuid-sandbox") 
chrome_options.add_argument("--remote-debugging-port=9222")

chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("start-maximized")
chrome_options.add_argument("disable-infobars")
chrome_options.add_argument("--disable-extensions")
chrome_prefs = {}
chrome_options.experimental_options["prefs"] = chrome_prefs
chrome_prefs["profile.default_content_settings"] = {"images": 2}

browser = webdriver.Chrome(options=chrome_options)

browser.implicitly_wait(10)
browser.set_page_load_timeout(timeout)

url='http://ident.me/'
browser.set_page_load_timeout(timeout)
browser.get(url)
browser.find_element_by_tag_name("body").text

from selenium import webdriver
import time 
status=False
error=None
timeout=30
proxy='104.139.74.25:34368'#, 'socks4'

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument('--user-agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36')
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--proxy-server=socks4://" + proxy)
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
chrome_options.add_argument("--disable-popup-blocking")
chrome_options.add_argument("--window-size=1420,1080")
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_prefs = {}
chrome_options.experimental_options["prefs"] = chrome_prefs
chrome_prefs["profile.default_content_settings"] = {"images": 2}
browser = webdriver.Chrome(options=chrome_options)

browser.get(site_url)
time.sleep(5)
browser.page_source
browser.quit()

---------------------------
from json import JSONDecoder

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


from selenium import webdriver
import time 
import json 
import pandas as pd
status=False
error=None
timeout=30
cat='Sold'
prop_id='131116802'
site_url='https://www.realestate.com.au/sold/property-house-nsw-cootamundra-131116802'
proxy='104.139.74.25:34368'#, 'socks4'

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--headless")
chrome_options.add_argument('--user-agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36')
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--proxy-server=socks4://" + proxy)
chrome_options.add_argument('--blink-settings=imagesEnabled=false')
chrome_options.add_argument("--disable-popup-blocking")
chrome_options.add_argument("--window-size=1420,1080")
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_prefs = {}
chrome_options.experimental_options["prefs"] = chrome_prefs
chrome_prefs["profile.default_content_settings"] = {"images": 2}
browser = webdriver.Chrome(options=chrome_options)

browser.get(site_url)
time.sleep(5)
page = browser.page_source
browser.quit()