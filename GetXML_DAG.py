# airflow bits here 
import airflow
from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import requests
import urllib.request
from fake_useragent import UserAgent 
     #scrape formatting
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from pandas import DataFrame
    #data saving
import os
#extract from gt file 
import gzip
import shutil 
try:
    from StringIO import StringIO
except:
    from io import StringIO

def ScrapeURL(baseurl,PagesavePath, **kwargs):  
    XMLsaveFile="XML_scrape_" + (datetime.now()).strftime('%Y-%m-%d') + '.xml'
    #create browser header for requests 
    ua = UserAgent()
    #print(ua.chrome)
    headers = {'User-Agent':str(ua.random)}
    #how many pages are there?
    #headers = { 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36', }
    response = requests.get(baseurl,headers=headers)
    y=BeautifulSoup(response.text, features="html.parser")
    #save xml to dir, will be read again later 
    # XMLFile=os.path.join(PagesavePath + "\\DL_Files\\", file.strip(' \t\n\r') )
    XmFileDir=os.path.join(PagesavePath, "DL_Files")
    try: 
        os.makedirs(XmFileDir)
        print("made dir: " + XmFileDir)
    except Exception as e: 
        # pass
        print("couldnt make dir: " + XmFileDir) 
        print(e)
    xmlFile=os.path.join(XmFileDir, XMLsaveFile)
    saveXML=open(xmlFile, "w")
    saveXML.write(y.prettify())
    saveXML.close()
    print("file saved to: " + xmlFile)
    
#https://stackoverflow.com/questions/19859282/check-if-a-string-contains-a-number
def hasNumbers(inputString):
    return any(char.isdigit() for char in inputString)

def ReadScrape(PagesavePath, **kwargs):
    XMLsaveFile="XML_scrape_" + (datetime.now()).strftime('%Y-%m-%d') + '.xml'
    CSVsaveFile="DF_Scrape_" + (datetime.now()).strftime('%Y-%m-%d') + '.csv'
    XMLDataset=pd.DataFrame(columns =['FileName', 'ScrapeDT', 'LastMod', 'Size', 'FileNo', 'Type'])
    #Read the XML file from dir 
    XmFileDir=os.path.join(PagesavePath, "DL_Files")
    try: 
        os.makedirs(XmFileDir)
        print("made dir: " + XmFileDir)
    except Exception as e:
        # pass
        print("couldnt make dir: " + XmFileDir) 
        print(e)

    xmlFile=os.path.join(XmFileDir, XMLsaveFile)
    XML= open(xmlFile, "r")
    content = XML.readlines()
    # Combine the lines in the list into a string
    content = "".join(content)
    RawURLFile = BeautifulSoup(content, "xml")
    for file in RawURLFile.find_all('contents'):
        if file.key != None: 
            FileID=0
            Type=""
            if hasNumbers(str(file.key.text)): #and "pdp" in str(file.key.text): 
                # changed get ID due to Python3 
                FileID=str( int( ''.join(x for x in str(file.key.text) if x.isdigit()) ) )#[1:]
                #get file type: buy, sold, rent
                if 'sold' in str(file.key.text).lower(): 
                    Type='sold'
                elif  'buy' in str(file.key.text).lower(): 
                    Type='buy'
                elif  'rent' in str(file.key.text).lower(): 
                    Type='rent'

            XMLDataset=XMLDataset.append({
                'ScrapeDT' : (datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
                , 'FileName' : str(file.key.text)
                , 'LastMod': str(file.lastmodified.text)
                , 'Size' : str(file.size.text)
                , 'FileNo': int(FileID)
                , 'Type' : str(Type)
                } ,ignore_index=True) 
    #write csv to file, to be viewed later 
    #pandas writes a 'csv' that looks like my financial future, sue me 
    CSVfile=os.path.join(XmFileDir, CSVsaveFile)
    XMLDataset.to_csv(CSVfile, index=False)

def WriteFile(PagesavePath, baseurl, **kwargs):
    XMLsaveFile="XML_scrape_" + (datetime.now()).strftime('%Y-%m-%d') + '.xml'
    CSVsaveFile="DF_Scrape_" + (datetime.now()).strftime('%Y-%m-%d') + '.csv'
    GoodWords=['rent','buy','sell','sold']
    XmFileDir=os.path.join(PagesavePath, "DL_Files")
    try: 
        os.makedirs(XmFileDir)
        print("made dir: " + XmFileDir)
    except Exception as e:
        # pass
        print("couldnt make dir: " + XmFileDir) 
        print(e)
    
    CSVfile=os.path.join(XmFileDir, CSVsaveFile)
    impCSV = pd.read_csv(CSVfile)
    for file in impCSV.FileName:
        #scrape on goodword 
        for words in GoodWords: 
            if words.lower() in file.lower():  
                dirFile=""
                #make pat
                dirFile= os.path.join(PagesavePath, "Extracts")
                dirFile= os.path.join(dirFile, file.strip(' \t\n\r') )
                # dirFile= os.path.join(PagesavePath + "/Extracts/" + file.strip(' \t\n\r') )
                #make folder, skip if exists 
                try: 
                    os.makedirs(dirFile)
                    print("made dir: " + dirFile)
                except Exception as e:
                    # pass
                    print("couldnt make dir: " + dirFile) 
                    print(e)
                
                gzFile= os.path.join(dirFile.strip(' \t\n\r'), file.strip(' \t\n\r'))
                xmlFile= os.path.join(dirFile.strip(' \t\n\r'), file.strip(' \t\n\r')[:-3])
                print("scaping from: " + baseurl + file.strip(' \t\n\r') )
                print("saving to: " + xmlFile)
                urllib.request.urlretrieve(baseurl + file.strip(' \t\n\r'), gzFile)
                
                #extracting contents of .gz file to dir 
                #https://stackoverflow.com/questions/15352668/download-and-decompress-gzipped-file-in-memory
                print("extracting file from: " + gzFile )
                print("saving xml as: " + xmlFile)
                with gzip.open(gzFile, 'rb') as f_in:
                    with open(xmlFile, 'wb') as f_out: 
                        
                        shutil.copyfileobj(f_in, f_out)

#dag stuff here 
args={
    'owner': 'Airflow'
    ,'start_date': airflow.utils.dates.days_ago(1)

}

dag = DAG(
    dag_id='GETXML_DAG'
    ,default_args=args
    ,schedule_interval=None

)

t1_Scrape_URL = PythonOperator(
    task_id="t1_Srape_URL"
    ,provide_context=True
    ,op_kwargs={
        'baseurl':'https://www.realestate.com.au/xml-sitemap/'
        , 'PagesavePath': '/usr/local/airflow/xmlSave'
        # , 'XMLsaveFile':'XML_scrape_' +
        }
    ,python_callable=ScrapeURL
    ,dag=dag
)

t2_Read_Scrape = PythonOperator(
    task_id="t2_Read_Scrape"
    ,provide_context=True
    ,op_kwargs={'PagesavePath': '/usr/local/airflow/xmlSave'
    }
    ,python_callable=ReadScrape
    ,dag=dag

)

t3_Wite_File = PythonOperator(
    task_id="t3_Wite_File"
    ,provide_context=True
    ,op_kwargs={
        'baseurl':'https://www.realestate.com.au/xml-sitemap/'
        , 'PagesavePath': '/usr/local/airflow/xmlSave'
    }
    ,python_callable=WriteFile
    ,dag=dag
)

t1_Scrape_URL >> t2_Read_Scrape >> t3_Wite_File