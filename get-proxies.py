# -*- coding: utf-8 -*-

import json,urllib.request
import time
import pyodbc 

req = urllib.request.Request(
    "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc", 
    data=None, 
    headers={
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
    }
)
data = urllib.request.urlopen(req).read()
output = json.loads(data)
cnxn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};"
                      "Server=LAPTOP-I91584GB\SQLEXPRESS;"
                      "Database=TextCorpuses;"
                      "Trusted_Connection=yes;"
                      "TrustServerCertificate=yes;")


print(output.get('data'))
for entry in output.get('data'):
    cursor = cnxn.cursor()
   
    cursor.execute("execute [dbo].[AddOrUpdateProxy] ?, ?, ?", (entry.get('ip'),entry.get('lastChecked'), entry.get('port')))
    cursor.close()
    cnxn.commit()
cnxn.close()
