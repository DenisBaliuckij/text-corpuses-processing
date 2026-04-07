import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="get_proxies_for_calls",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs = 1,
    tags=["proxies"],
) as dag:

    @task()
    def download_proxy_list():
        import json,urllib.request
        import time
        import pyodbc 
        import configs
        from configs import getConfig

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
            cnxn = pyodbc.connect(getConfig()["ConnectionString"])

            for entry in output.get('data'):
                cursor = cnxn.cursor()
                cursor.execute("execute [dbo].[AddOrUpdateProxy] @ip = ?, @port = ?, @lastChecked = ?, @protocols = ?", (entry.get('ip'), entry.get('port'), entry.get('lastChecked'), ''.join(entry.get('protocols'))))
                cursor.close()
                cnxn.commit()
            cnxn.close()
            total = output.get('total')
            if(limit*page > total):
                break
            page+=1
        
    download_proxy_list()
