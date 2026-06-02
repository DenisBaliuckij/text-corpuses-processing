import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="get_proxies_for_calls_2",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs = 1,
    tags=["proxies"],
) as dag:

    @task()
    def download_proxy_list_2():
       from repositories.proxy_repository import ProxyRepository
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
               ProxyRepository.add_or_update(str(ipValue.text), int(portValue.text), timestamp, 'http')
          
        
    download_proxy_list_2()
