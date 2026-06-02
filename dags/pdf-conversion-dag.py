import pendulum

from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="pdf_conversion",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["latexFiles"],
) as dag:

    @task()
    def convertPdfFiles():
        import pdfConverter
        converted = pdfConverter.run_conversion()
        print(f"Converted {converted} files")

    convertPdfFiles()
