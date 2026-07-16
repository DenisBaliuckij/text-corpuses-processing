import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="get_proxies_for_calls_5",
    schedule ="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs = 1,
    tags=["proxies"],
) as dag:

    @task()
    def download_proxy_list_5():
        # Proxifly (github.com/proxifly/free-proxy-list) - refreshes every
        # 5 minutes, served via jsDelivr's CDN mirror of the repo so it's
        # fast and doesn't hit GitHub's raw-file rate limits. Added
        # 2026-07-16 as part of broadening free-proxy sourcing beyond
        # geonode/proxydb/proxyscrape/free-proxy-list.net, which independent
        # testing (and this pipeline's own experience) shows convert at
        # low single-digit percent or worse.
        import json
        import urllib.request
        import pendulum
        from proxyValidator import validate_and_import

        url = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.json"
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
        entries = json.loads(data)

        candidates = []
        for entry in entries:
            ip = entry.get('ip')
            port = entry.get('port')
            protocol = entry.get('protocol') or 'http'
            if not ip or not port:
                continue
            try:
                candidates.append((ip, int(port), protocol))
            except (TypeError, ValueError):
                continue

        timestamp = int(pendulum.now('UTC').timestamp())
        imported = validate_and_import(candidates, timestamp)
        print(f"Imported {imported}/{len(candidates)} validated proxies")

    download_proxy_list_5()
