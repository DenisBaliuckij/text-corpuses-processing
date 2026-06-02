# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 21:30:56 2026

@author: denis
"""

# -*- coding: utf-8 -*-
"""
Created on Sun Jan 25 13:11:31 2026

@author: denis
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dags'))

import json
import requests
import bs4
from repositories.proxy_repository import ProxyRepository
from repositories.pdf_repository import PdfRepository
from repositories.service_state_repository import ServiceStateRepository


while True:
    serviceID = 2

   

    state = {
        "pageNumber" : 1
        }

    fetchResults = ServiceStateRepository.get(serviceID)
    if fetchResults is not None:
        state = json.loads(fetchResults[0])
#print(str(proxieIp) + str(proxiePort) + str(proxieProtocol))
    
    proxieResult = ProxyRepository.get_latest()
    print(proxieResult)
    proxieIp = proxieResult["proxieIp"]
    proxiePort = proxieResult["proxiePort"]
    proxieProtocol = proxieResult["proxieProtocol"]

# Send a GET request to the URL

    proxies = {'http': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort),
                'https': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort)}

    print(proxies)

    try:
        response = requests.get('https://link.springer.com/search?query=&content-type=Article&openAccess=true&sortBy=relevance&page='+str(state["pageNumber"]), 
                                data=None, 
                                headers={
                                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
                                    }, 
                                proxies=proxies, 
                                timeout=30)
    except Exception as e:
        print(e)
        ProxyRepository.mark_broken(str(proxieIp).strip())
        continue
            
            
    print(response.text)
    soup = bs4.BeautifulSoup(response.text, "html.parser")

    for url in soup.find_all("a"):
        try:
            if "article" in url["href"]:
                PdfRepository.add_url("https://link.springer.com/"+str(url["href"]).strip() + ".pdf")
                
        except Exception as e:
            print(e)
        finally:
            print("Url discovered", url)

    state["pageNumber"] =state["pageNumber"]+1
    if(state["pageNumber"]>1000000):
        break
        
    ServiceStateRepository.update(serviceID, json.dumps(state))
ServiceStateRepository.remove(serviceID)

