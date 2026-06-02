# -*- coding: utf-8 -*-
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dags'))

import json, urllib.request
import time
from repositories.proxy_repository import ProxyRepository

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
        ProxyRepository.add_or_update(entry.get('ip'), entry.get('port'), entry.get('lastChecked'), ''.join(entry.get('protocols')))

    total = output.get('total')
    if(limit*page > total):
        break
    page+=1