import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import mods.db_import as db_import 
import mods.proxy as proxy 
import mods.scrape_site as scrape_site
site_url='https://www.realestate.com.au/sold/property-residential+land-nt-acacia+hills-201194009'
timeout=5
sleep_time=4
from selenium import webdriver
import time 
import json 
import pandas as pd

start_time = time.time()
scrape_status=False
while scrape_status==False:
    try:
        prox, proxy_type = scrape_site.workProxy()
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
        scrape_status=True
    except Exception as e: 
        print('error: ', str(e))

print("selenium headers s:", time.time() - start_time)