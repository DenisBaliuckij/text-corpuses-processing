# -*- coding: utf-8 -*-
"""
Created on Sun Jan 25 13:11:31 2026

@author: denis
"""

import requests
import pyodbc 
import requests
import bs4



cnxn = pyodbc.connect("Driver={ODBC Driver 18 for SQL Server};"
                      "Server=LAPTOP-I91584GB\SQLEXPRESS;"
                      "Database=TextCorpuses;"
                      "Trusted_Connection=yes;"
                      "TrustServerCertificate=yes;")
"""
proxies = {'http': 'socks5://user:pass@host:port',
           'https': 'socks5://user:pass@host:port'}
cursor = cnxn.cursor()
cursor.execute("execute [dbo].[GetLatestProxy]")
proxieIp = cursor.fetchone()[0]
print(proxieIp)
cursor.close()
cnxn.close()
"""
# Send a GET request to the URL
#proxies = {'https': 'http://' + str(proxieIp)+ +}

response = requests.get('https://arxiv.org/search/?query=a&searchtype=all&abstracts=show&order=-announced_date_first&size=50&start=0')


#response = requests.get('https://ip.me/', proxies)



# Print the HTML content of the page

print(response.text)
soup = bs4.BeautifulSoup(response.text, "html.parser")
# Creating a list to store results in.
urlsContainingWord = []

# Get all the URLs in the page containing the word.
for url in soup.find_all("a"):
    try:
        if "pdf" in url["href"]:
            urlsContainingWord.append(url["href"])
    except Exception as e:
        print(e)
    finally:
        print("Url discovered", url)
# Print out the result.
print(urlsContainingWord)
