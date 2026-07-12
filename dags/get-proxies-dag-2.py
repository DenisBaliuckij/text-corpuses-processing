import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="get_proxies_for_calls_2",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs = 1,
    tags=["proxies"],
) as dag:

    @task()
    def download_proxy_list_2():
       import requests
       import bs4
       import pendulum
       from proxyValidator import validate_and_import

       offset = 0
       candidates = []
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

               portCell = lines[1]
               portValue = portCell.find_all("a")[0]

               try:
                   candidates.append((str(ipValue.text).strip(), int(portValue.text.strip()), 'http'))
               except ValueError:
                   continue

           offset += 30

       timestamp = int(pendulum.now('UTC').timestamp())
       imported = validate_and_import(candidates, timestamp)
       print(f"Imported {imported}/{len(candidates)} validated proxies")

    download_proxy_list_2()
