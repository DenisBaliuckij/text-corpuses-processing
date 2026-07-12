import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="get_proxies_for_calls",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs = 1,
    tags=["proxies"],
) as dag:

    @task()
    def download_proxy_list():
        import json
        import urllib.request
        import pendulum
        from proxyValidator import validate_and_import

        page = 1
        limit = 500
        candidates = []
        while True:
            url = (
                "https://proxylist.geonode.com/api/proxy-list"
                f"?limit={limit}&page={page}&sort_by=lastChecked&sort_type=desc"
            )
            req = urllib.request.Request(
                url,
                data=None,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) '
                                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/35.0.1916.47 Safari/537.36'
                }
            )
            data = urllib.request.urlopen(req).read()
            output = json.loads(data)

            for entry in output.get('data', []):
                ip = entry.get('ip')
                port = entry.get('port')
                protocols = entry.get('protocols', [])
                if not ip or not port or not protocols:
                    continue
                try:
                    candidates.append((ip, int(port), protocols[0]))
                except (TypeError, ValueError):
                    continue

            total = output.get('total', 0)
            if limit * page > total:
                break
            page += 1

        timestamp = int(pendulum.now('UTC').timestamp())
        imported = validate_and_import(candidates, timestamp)
        print(f"Imported {imported}/{len(candidates)} validated proxies")

    download_proxy_list()
