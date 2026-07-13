# -*- coding: utf-8 -*-
import pendulum
from airflow.sdk import DAG
from airflow.sdk import task

with DAG(
    dag_id="custom_query_processor",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["pdfUrls", "customQuery"],
) as dag:

    @task()
    def process_custom_queries():
        import json
        import re
        import ftpConnector
        from ftpConnector import ftpConnector
        from repositories.custom_query_repository import CustomQueryRepository
        from repositories.pdf_repository import PdfRepository
        from arxivApiDownloader import fetch_page as arxiv_fetch_page
        from pubmedDownloader import fetch_page as pubmed_fetch_page
        from semanticScholarDownloader import fetch_page as semantic_scholar_fetch_page
        from archiveOrgDownloader import search_pdfs as archive_org_search_pdfs
        from shodhgangaDownloader import search_pdfs as shodhganga_search_pdfs

        STATUS_CREATED = 0
        STATUS_FULFILLING = 20
        STATUS_COMPLETED = 30
        STATUS_ERROR = 99

        PDF_STATUS_PENDING = 0
        PDF_STATUS_COPIED = 10
        PDF_STATUS_DOWNLOADED = 20
        PDF_STATUS_FAILED = 99

        MAX_PAGES = 40  # safety cap on discovery pagination for one query

        def archive_org_fetch_page(criterion, page, proxy):
            ROWS = 50
            urls, has_more = archive_org_search_pdfs(criterion['query'], page, ROWS, proxies=None, tag='')
            max_results = criterion.get('max_results', 1000)
            has_more = has_more and (page * ROWS) < max_results
            return urls, has_more

        def shodhganga_fetch_page(criterion, page, proxy):
            RPP = 50
            urls, has_more = shodhganga_search_pdfs(
                criterion['subject'], page, RPP, proxies=None, tag='',
                language=criterion.get('language', 'Gujarati'),
            )
            max_results = criterion.get('max_results', 1000)
            has_more = has_more and (page * RPP) < max_results
            return urls, has_more

        ADAPTERS = {
            'arxiv': arxiv_fetch_page,
            'pubmed': pubmed_fetch_page,
            'semantic_scholar': semantic_scholar_fetch_page,
            'archive_org': archive_org_fetch_page,
            'shodhganga': shodhganga_fetch_page,
        }

        def folder_slug(query_id, folder_name):
            slug = re.sub(r'[^a-zA-Z0-9_-]', '_', folder_name)
            return f'{query_id}_{slug}'

        def discover_and_enqueue(query_id, source_name, criterion, folder_name):
            adapter = ADAPTERS.get(source_name)
            if adapter is None:
                raise ValueError(f'unknown source {source_name!r}')

            slug = folder_slug(query_id, folder_name)
            page = 1
            has_more = True
            while has_more and page <= MAX_PAGES:
                urls, has_more = adapter(criterion, page, None)
                for url in urls:
                    base_url = url.split('#')[0]
                    existing_location = CustomQueryRepository.find_existing_download(base_url)
                    pdf_id = CustomQueryRepository.add_pdf(query_id, base_url)
                    if existing_location:
                        basename = existing_location.rsplit('/', 1)[-1]
                        destination = f'custom/{slug}/{basename}'
                        file_obj = ftpConnector.getFile(existing_location)
                        ftpConnector.storeFile(destination, file_obj)
                        CustomQueryRepository.mark_pdf_status(pdf_id, PDF_STATUS_COPIED, destination)
                    else:
                        PdfRepository.add_url(f'{base_url}#customquery_{slug}')
                page += 1

        def check_pending_downloads(query_id):
            for pdf_id, pdf_url in CustomQueryRepository.get_pending_downloads(query_id):
                location = CustomQueryRepository.find_existing_download(pdf_url)
                if location and location != 'NA':
                    CustomQueryRepository.mark_pdf_status(pdf_id, PDF_STATUS_DOWNLOADED, location)

        def is_query_complete(query_id):
            rows = CustomQueryRepository.get_pdfs(query_id)
            return all(r[2] != PDF_STATUS_PENDING for r in rows)

        for row in CustomQueryRepository.get_pending():
            query_id, source_name, criterion_json, folder_name = row[0], row[1], row[2], row[3]
            status = row[4]
            try:
                if status == STATUS_CREATED:
                    # Discovery runs exactly once, on the first pickup - the
                    # status transition below (to FULFILLING) is what stops
                    # every subsequent @continuous loop iteration from
                    # re-paginating the source API from scratch.
                    criterion = json.loads(criterion_json)
                    discover_and_enqueue(query_id, source_name, criterion, folder_name)

                check_pending_downloads(query_id)
                if is_query_complete(query_id):
                    CustomQueryRepository.update_query_status(query_id, STATUS_COMPLETED)
                else:
                    CustomQueryRepository.update_query_status(query_id, STATUS_FULFILLING)
            except Exception as e:
                CustomQueryRepository.update_query_status(query_id, STATUS_ERROR, str(e))

    process_custom_queries()
