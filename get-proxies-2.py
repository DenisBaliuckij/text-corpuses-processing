# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 10:29:21 2026

@author: denis
"""

# -*- coding: utf-8 -*-

import json,urllib.request
import time
import pyodbc 
import dbConnector
from dbConnector import databaseConnector
import requests
import bs4
import pendulum

offset = 0
while True:
    if(offset>1000):
        break
    url = "https://proxydb.net/?country=&offset="+str(offset)+"&protocol=http"
    response = requests.get(
        url, 
        data=None, 
        headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        })
   
    
    soup = bs4.BeautifulSoup(response.text, "html.parser")
   
    for line in soup.find_all("tr"):
        lines = line.find_all("td")
        if len(lines) < 2:
            continue
        ipCell = lines[0]
        ipValue = ipCell.find_all("a")[0]
        
        print(ipValue.text)
        
        portCell = lines[1]
        portValue = portCell.find_all("a")[0]
        
        print(portValue.text)
        timestamp = int(pendulum.now('UTC').timestamp())
        print(timestamp)
        databaseConnector.addOrUpdateProxy(str(ipValue.text), int(portValue.text), timestamp, 'http')
   