# -*- coding: utf-8 -*-

import json,urllib.request
import time
import pyodbc 
import dbConnector
from dbConnector import databaseConnector

page = 1
limit = 500
while True:
    url = "https://proxylist.geonode.com/api/proxy-list?limit=" + str(limit)+ "&page=" + str(page)+ "&sort_by=lastChecked&sort_type=desc"
    req = urllib.request.Request(
        url, 
        data=None, 
        headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        })
    data = urllib.request.urlopen(req).read()
    output = json.loads(data)
   

    for entry in output.get('data'):
        databaseConnector.addOrUpdateProxy(entry.get('ip'), entry.get('port'), entry.get('lastChecked'), ''.join(entry.get('protocols')))

    total = output.get('total')
    if(limit*page > total):
        break
    page+=1