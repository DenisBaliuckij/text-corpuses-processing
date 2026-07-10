import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="get_proxies_for_calls_4",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs = 1,
    tags=["proxies"],
) as dag:

    @task()
    def download_proxy_list_4():
        import requests
        import bs4
        import pendulum
        from proxyValidator import validate_and_import

        response = requests.get(
            "https://free-proxy-list.net/",
            data=None,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
            },
            timeout=30,
        )
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        timestamp = int(pendulum.now('UTC').timestamp())

        candidates = []
        for row in soup.select("table tbody tr"):
            cells = row.find_all("td")
            if len(cells) < 7:
                continue
            ip = cells[0].text.strip()
            port = cells[1].text.strip()
            https_col = cells[6].text.strip().lower()
            protocol = 'https' if https_col == 'yes' else 'http'
            try:
                candidates.append((ip, int(port), protocol))
            except ValueError:
                continue

        imported = validate_and_import(candidates, timestamp)
        print(f"Imported {imported}/{len(candidates)} validated proxies")

    download_proxy_list_4()
