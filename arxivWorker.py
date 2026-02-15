# -*- coding: utf-8 -*-
"""
Created on Sun Jan 25 13:11:31 2026

@author: denis
"""

import requests
import pyodbc 
import requests
import bs4
i=1
for s in "abcdefghijklmnopqrstuvwxyz":
    while True:
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
#print(str(proxieIp) + str(proxiePort) + str(proxieProtocol))
    
    

# Send a GET request to the URL

        proxies = {'http': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort),
                   'https': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort)}

        print(proxies)
        print(str(s) + ' ' + str(i) )
        try:
            response = requests.get('https://arxiv.org/search/?query='+s+'&searchtype=all&abstracts=show&order=-announced_date_first&size=50&start='+str(i), 
                                    data=None, 
                                    headers={
                                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
                                        }, 
                                    proxies=proxies, 
                                    timeout=30)
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
            
            
        print(response.text)
        soup = bs4.BeautifulSoup(response.text, "html.parser")

        for url in soup.find_all("a"):
            try:
                if "pdf" in url["href"]:
                    cnxn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};"
                                          "Server=LAPTOP-I91584GB\SQLEXPRESS;"
                                          "Database=TextCorpuses;"
                                          "Trusted_Connection=yes;"
                                          "TrustServerCertificate=yes;")
                    cursor = cnxn.cursor()
                    cursor.execute("execute [dbo].[AddPdfUrl] @url = ?", (str(url["href"]).strip()))
                    cursor.close()
                    cnxn.commit()
                    cnxn.close()
            except Exception as e:
                print(e)
            finally:
                print("Url discovered", url)

        i+=1
        if(i>5000):
            i=0
            break

cnxn.close()