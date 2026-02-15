# -*- coding: utf-8 -*-
from pathlib import Path
import requests
import pyodbc 
i = 0

while i<500:
    try:
        filename = Path('c:/pdfs/'+str(i)+'.pdf')
        cnxn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};"
                              "Server=LAPTOP-I91584GB\SQLEXPRESS;"
                              "Database=TextCorpuses;"
                              "Trusted_Connection=yes;"
                              "TrustServerCertificate=yes;")
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetPdfToDownload]")
        fetchResults = cursor.fetchone()
        url = fetchResults[0]
        cursor.close()
        cnxn.close()
        cnxn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};"
                              "Server=LAPTOP-I91584GB\SQLEXPRESS;"
                              "Database=TextCorpuses;"
                              "Trusted_Connection=yes;"
                              "TrustServerCertificate=yes;")
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[GetLatestProxy]")
        fetchResults = cursor.fetchone()
        proxieIp = fetchResults[0]
        proxiePort = fetchResults[1]
        proxieProtocol = fetchResults[2]
        cursor.close()
        cnxn.close()
        proxies = {'http': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort),
                   'https': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort)}

        print(proxies)

        response = requests.get(url, 
                            data=None, 
                            headers={
                                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
                                }, 
                            proxies=proxies, 
                            timeout=30)
        filename.write_bytes(response.content)
        cnxn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};"
                              "Server=LAPTOP-I91584GB\SQLEXPRESS;"
                              "Database=TextCorpuses;"
                              "Trusted_Connection=yes;"
                              "TrustServerCertificate=yes;")
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[SavePdfFileLocation] @pdfUrl = ?, @fileLocation=?", (url, filename))
        cnxn.commit()
        cursor.close()
        cnxn.close()
    except Exception as e:
        print(e)
        cnxn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};"
                              "Server=LAPTOP-I91584GB\SQLEXPRESS;"
                              "Database=TextCorpuses;"
                              "Trusted_Connection=yes;"
                              "TrustServerCertificate=yes;")
        cursor = cnxn.cursor()
        cursor.execute("execute [dbo].[MarkProxyAsBroken] @ip = ?", (str(proxieIp).strip()))
        cnxn.commit()
        cursor.close()
        cnxn.close()
        continue
    i+=1
    break
