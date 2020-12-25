
def ScrapeURL(baseurl, RootDir, PageSaveXML):  
    XMLsaveFile="XML_sitemap_" + (datetime.datetime.now()).strftime('%Y-%m-%d') + '.xml'
    headers = { 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36', }
    response = requests.get(baseurl,headers=headers)
    XmFileDir=os.path.join(RootDir, PageSaveXML)
    print("save path:", str(XmFileDir))
    xmlFile=os.path.join(XmFileDir, XMLsaveFile)
    print("xmlFile is: ", xmlFile)
    print(response.text)
    print("folder check for folder:", XmFileDir, os.path.isdir(XmFileDir) )
    time.sleep(600)
    # try: 
    #     os.makedirs(XmFileDir)
    #     print("made dir: " + XmFileDir)
    # except Exception as e: 
    #     # pass
    #     print("couldnt make dir: " + XmFileDir) 
    #     print(e)   
    saveXML=open(xmlFile, "w")
    saveXML.write(response.text)
    saveXML.close()
    print("file saved to: " + xmlFile)

def main():
    baseurl='https://www.realestate.com.au/xml-sitemap/'
    RootDir='/usr/local/airflow/xmlsave'
    PageSaveXML='DL_Files/DL_Landing'
    ScrapeURL(baseurl,RootDir,PageSaveXML)
    