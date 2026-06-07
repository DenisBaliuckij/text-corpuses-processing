import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="get_arxiv_urls",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs = 1,
    tags=["pdfUrls"],
) as dag:

    @task()
    def getArxivPdfUrls():
       import json
       import requests
       import bs4
       from repositories.proxy_repository import ProxyRepository
       from repositories.pdf_repository import PdfRepository
       from repositories.service_state_repository import ServiceStateRepository

       serviceID = 1

       letters = "abcdefghijklmnopqrstuvwxyz "

       state = {
           "pageNumber" : 1,
           "letter" : 'a'
           }

       fetchResults = ServiceStateRepository.get(serviceID)
       if fetchResults is not None:
           state = json.loads(fetchResults[0])


       while letters.find(state["letter"]) < len(letters)-1:
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
               response = requests.get('https://arxiv.org/search/?query='+state["letter"]+'&searchtype=all&abstracts=show&order=-announced_date_first&size=50&start='+str(state["pageNumber"]),
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
                   if "pdf" in url["href"]:
                       PdfRepository.add_url(str(url["href"]).strip())
               except Exception as e:
                   print(e)
               finally:
                   print("Url discovered", url)

           state["pageNumber"] =state["pageNumber"]+1
           if(state["pageNumber"]>5000):
               state["pageNumber"]=1
               state["letter"] = letters[letters.find(state["letter"])+1]
           ServiceStateRepository.update(serviceID, json.dumps(state))
       ServiceStateRepository.remove(serviceID)
        
    getArxivPdfUrls()
