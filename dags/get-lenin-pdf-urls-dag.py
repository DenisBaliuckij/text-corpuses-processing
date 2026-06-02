import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="get_lenin_urls",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs = 1,
    tags=["pdfUrls"],
) as dag:

    @task()
    def getLeninPdfUrls():

        import json
        import requests
        import bs4
        from repositories.proxy_repository import ProxyRepository
        from repositories.pdf_repository import PdfRepository
        from repositories.service_state_repository import ServiceStateRepository

        serviceID = 3
        state = {
                "currentLink":0,
                "links":[],
                "pageNumber":1
             }
        fetchResults = ServiceStateRepository.get(serviceID)
        if fetchResults is not None:
            state = json.loads(fetchResults[0])
        else:
            while True:
                proxieResult = ProxyRepository.get_latest()
                print(proxieResult)
                proxieIp = proxieResult["proxieIp"]
                proxiePort = proxieResult["proxiePort"]
                proxieProtocol = proxieResult["proxieProtocol"]
                proxies = {'http': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort),
                            'https': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort)}
                try:
                     response = requests.get('https://cyberleninka.ru/article',
                                            data=None,
                                            headers={
                                                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
                                                },
                                            proxies=proxies,
                                            timeout=30)
                     soup = bs4.BeautifulSoup(response.text, "html.parser")
                     for url in soup.find_all("a"):
                         try:
                             if "article" in url["href"]:
                                 state["links"].append(url["href"])
                         except Exception as e:
                             print(e)
                         finally:
                             print("Url discovered", url)
                     if len(state["links"]) < 1:
                         continue
                     ServiceStateRepository.update(serviceID, json.dumps(state))
                     break
                except Exception as e:
                    print(e)
                    ProxyRepository.mark_broken(str(proxieIp).strip())
                    continue

        while True:

            rubric = state["links"][state["currentLink"]]
            try:
                proxieResult = ProxyRepository.get_latest()
                print(proxieResult)
                proxieIp = proxieResult["proxieIp"]
                proxiePort = proxieResult["proxiePort"]
                proxieProtocol = proxieResult["proxieProtocol"]

            # Send a GET request to the URL

                proxies = {'http': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort),
                            'https': proxieProtocol.strip() + '://' + str(proxieIp).strip() + ':' + str(proxiePort)}

                print(proxies)
                response = requests.get('https://cyberleninka.ru/'+rubric+"/"+str(state["pageNumber"]),
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
            urlCounter = 0
            for url in soup.find_all("a"):
                try:
                    if "article" in url["href"]:
                        PdfRepository.add_url("https://cyberleninka.ru/"+str(url["href"]).strip() + "/pdf")
                        urlCounter+=1
                except Exception as e:
                    print(e)
                finally:
                    print("Url discovered", url)
            if urlCounter>0:
                state["pageNumber"] =state["pageNumber"]+1
            else:
                if state["currentLink"] > len(state["links"]):
                    ServiceStateRepository.remove(serviceID)
                    break
                state["currentLink"] +=1
            ServiceStateRepository.update(serviceID, json.dumps(state))
        
    getLeninPdfUrls()
