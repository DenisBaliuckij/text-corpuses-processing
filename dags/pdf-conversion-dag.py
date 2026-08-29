import pendulum

from airflow.models import Variable
from airflow.sdk import DAG
from airflow.sdk import task


with DAG(
    dag_id="pdf_conversion",
    schedule="@continuous",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["latexFiles"],
) as dag:

    @task()
    def convertPdfFiles():
        import pdfConverter
        phase_str = Variable.get("pdf_conversion_phase", default_var=None)
        phase = int(phase_str) if phase_str else None
        converted = pdfConverter.run_conversion(phase=phase)
        print(f"Converted {converted} files (phase={phase})")

    convertPdfFiles()
