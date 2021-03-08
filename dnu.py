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
start_time = time.time()
status=False
error=None
timeout=30
cat='Sold'
prop_id='131116802'
site_url='https://www.realestate.com.au/sold/property-house-nsw-cootamundra-131116802'
proxy='185.141.215.61:1080'#, 'socks4'

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
prefs = {"profile.managed_default_content_settings.images": 2}
chrome_options.add_experimental_option("prefs", prefs)
browser = webdriver.Chrome(options=chrome_options)
print("loaded chrome")

browser.get(site_url)
print("loaded page")
time.sleep(5)
page = browser.page_source
print("loaded html")
browser.quit()

all_json = extract_json_objects(page)
longest=0
_max=''
long_count=0
for result in all_json:
    if len(result) > longest: 
        longest=len(result)
        _max=result
    long_count+=1
print("loaded json")

#save vales to dir to be safe
with open(cat + '_' + prop_id + '.html','x') as file: #html
    file.write(page)

with open(cat + '_' + prop_id + '.json','x') as file: #json
    file.write(json.dumps(_max, ensure_ascii=False, sort_keys=True))

#now we get writable variables 
#address 
try:
    _ad_sub=_max['$' + cat + 'ResidentialListing' + prop_id + '.address']['suburb']
except: 
    _ad_sub=''

try:
    _ad_post=_max['$' + cat + 'ResidentialListing' + prop_id + '.address']['postcode']
except: 
    _ad_post=''

try:
    _ad_state=_max['$' + cat + 'ResidentialListing' + prop_id + '.address']['state']
except:
    _ad_state=''

try:
    _ad_full=_max['$' + cat + 'ResidentialListing' + prop_id + '.address.display']['fullAddress']
except:
    _ad_full=''

try:
    _ad_short=_max['$' + cat + 'ResidentialListing' + prop_id + '.address.display']['shortAddress']
except: 
    _ad_short=''

#land
try:
    _ad_ls=_max['$' + cat + 'ResidentialListing' + prop_id + '.propertySizes.land']['displayValue']
except: 
    _ad_ls=''

try:
    _ad_ls_unit=_max['$' + cat + 'ResidentialListing' + prop_id + '.propertySizes.land.sizeUnit']['displayValue']
except: 
    _ad_ls_unit=''

try:    
    _ad_ls_type=_max['$' + cat + 'ResidentialListing' + prop_id + '.propertySizes.preferred']['sizeType']
except: 
    _ad_ls_type=''

#geospatial 
try:
    _ad_lat=_max['$' + cat + 'ResidentialListing' + prop_id + '.address.display.geocode']['latitude']
except: 
    _ad_lat=''

try:
    _ad_long=_max['$' + cat + 'ResidentialListing' + prop_id + '.address.display.geocode']['longitude']
except: 
    _ad_long=''

#prop atts
try:
    _ad_bed=_max['$' + cat + 'ResidentialListing' + prop_id + '.generalFeatures.bedrooms']['value']
except: 
    _ad_bed=''

try:
    _ad_bath=_max['$' + cat + 'ResidentialListing' + prop_id + '.generalFeatures.bathrooms']['value']
except: 
    _ad_bath=''

try:
    _ad_park=_max['$' + cat + 'ResidentialListing' + prop_id + '.generalFeatures.parkingSpaces']['value']
except: 
    _ad_park=''

try:
    _ad_type=_max['$' + cat + 'ResidentialListing' + prop_id + '.propertyType']['id']
except: 
    _ad_type=''

#price
try:
    _ad_price=_max['$' + cat + 'ResidentialListing' + prop_id + '.price']['display']
except: 
    _ad_price=''

#agency 
try:
    _ag_name=_max['$' + cat + 'ResidentialListing' + prop_id + '.listingCompany']['name']
except: 
    _ag_name=''

try:
    _ag_address=_max['$' + cat + 'ResidentialListing' + prop_id + '.listingCompany.address.display']['fullAddress']
except: 
    _ag_address=''

try:
    _ag_id=_max['$' + cat + 'ResidentialListing' + prop_id + '.listingCompany']['id']
except: 
    _ag_id=''

try:
    _ag_type=_max['$' + cat + 'ResidentialListing' + prop_id + '.listingCompany']['__typename']
except: 
    _ag_type=''

try:
    _ag_ph=_max['$' + cat + 'ResidentialListing' + prop_id + '.listingCompany']['businessPhone']
except: 
    _ag_ph=''

#agent 
try:
    _agn_name=_max[cat + 'ResidentialListing' + prop_id+ '.listers.0']['name']
except: 
    _agn_name=''

#agent 
try:
    _agn_ph=_max['$' + cat + 'ResidentialListing' + prop_id+ '.listers.0.phoneNumber']['display']
except: 
    _agn_ph=''

try:
    _agn_id=_max[cat + 'ResidentialListing' + prop_id+ '.listers.0']['id']
except: 
    _agn_id=''

try:
    _agn_agent_id=_max[cat + 'ResidentialListing' + prop_id+ '.listers.0']['display']
except: 
    _agn_agent_id=''

print("loaded vars")
_result = pd.DataFrame([{
    'ad_sub': _ad_sub
    ,'ad_post': _ad_post
    ,'ad_state': _ad_state
    ,'ad_full': _ad_full
    ,'ad_short': _ad_short
    ,'ad_ls': _ad_ls
    ,'ad_ls_unit': _ad_ls_unit
    ,'ad_ls_type': _ad_ls_type
    ,'ad_geo_lat': _ad_lat
    ,'ad_geo_long': _ad_long
    ,'ad_bed': _ad_bed
    ,'ad_bath': _ad_bath
    ,'ad_park': _ad_park
    ,'ad_type': _ad_type
    ,'ad_price': _ad_price
    ,'ag_name': _ag_name
    ,'ag_address': _ag_address
    ,'ag_id': _ag_id
    ,'ag_type': _ag_type
    ,'ag_ph': _ag_ph
    ,'agn_name': _agn_name
    ,'agn_ph': _agn_ph
    ,'agn_id': _agn_id
    ,'agn_agent_id': _agn_agent_id
}])
_result.to_csv('test.csv')
print("loaded csv")
print("My program took", time.time() - start_time, "to run")