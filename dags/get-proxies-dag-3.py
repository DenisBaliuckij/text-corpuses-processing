import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="get_proxies_for_calls_3",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs = 1,
    tags=["proxies"],
) as dag:

    @task()
    def download_proxy_list_3():
        import urllib.request
        import pendulum
        from proxyValidator import validate_and_import

        url = (
            "https://api.proxyscrape.com/v2/?request=getproxies"
            "&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
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
        data = urllib.request.urlopen(req).read().decode()
        timestamp = int(pendulum.now('UTC').timestamp())

        candidates = []
        for line in data.strip().splitlines():
            line = line.strip()
            if not line or ':' not in line:
                continue
            ip, _, port = line.partition(':')
            try:
                candidates.append((ip.strip(), int(port.strip()), 'http'))
            except ValueError:
                continue

        imported = validate_and_import(candidates, timestamp)
        print(f"Imported {imported}/{len(candidates)} validated proxies")

    download_proxy_list_3()
