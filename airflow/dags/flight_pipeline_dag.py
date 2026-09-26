from datetime import datetime, timedelta
import sys
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Path to project source code inside Docker
sys.path.insert(0, "/opt/airflow/src")
from extract_historical import run as extract_run, get_random_days_back

# السماح لـ Airflow باستيراد ملفات src/
sys.path.append(str(Path("/opt/airflow/src")))


default_args = {
    "owner": "athee",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id="flight_data_pipeline",
    description="Pipeline : OpenSky -> S3 -> Glue -> dbt",
    default_args=default_args,
    start_date=datetime(2026, 9, 1),
    schedule="@daily",
    catchup=False,
    tags=["flight-data"],
) as dag:

    # ==========================================
    # 1. Extract historical flights
    # ==========================================

    def run_extraction():
      random_start = get_random_days_back()
      extract_run(num_windows=1, days_back_start=random_start)


    extract_historical_task = PythonOperator(
        task_id="extract_historical_flights",
        python_callable=run_extraction,
    )


    # ==========================================
    # 2. Upload data to S3
    # ==========================================

    def run_upload():
        from upload_to_s3 import run as upload_run
        upload_run()


    upload_to_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=run_upload,
    )


    # ==========================================
    # 3. Run AWS Glue Job
    # ==========================================

    run_glue_task = GlueJobOperator(
    task_id="run_glue_cleaning_job",
    job_name="flight-raw-to-processed",
    region_name="us-east-1",
    aws_conn_id="aws_default",
    wait_for_completion=True,
    )


    # ==========================================
    # 4. Run dbt
    # ==========================================

    run_dbt_run_task = DockerOperator(
        task_id="dbt_run",
        image="ghcr.io/dbt-labs/dbt-snowflake:1.8.latest",
        command=["run", "--profiles-dir", "/root/.dbt", "--project-dir", "/usr/app"],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,
        environment={"DBT_PRIVATE_KEY_PATH": "/root/.dbt/snowflake_key.p8"},
        mounts=[
            Mount(
                source="C:\\Users\\athee\\Projects\\flight-data-project\\flight_transforms",
                target="/usr/app",
                type="bind",
            ),
            Mount(
                source="C:\\Users\\athee\\.dbt",
                target="/root/.dbt",
                type="bind",
            ),
        ],
    )

    run_dbt_test_task = DockerOperator(
        task_id="dbt_test",
        image="ghcr.io/dbt-labs/dbt-snowflake:1.8.latest",
        command=["test", "--profiles-dir", "/root/.dbt", "--project-dir", "/usr/app"],
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,
        environment={"DBT_PRIVATE_KEY_PATH": "/root/.dbt/snowflake_key.p8"},
        mounts=[
            Mount(
                source="C:\\Users\\athee\\Projects\\flight-data-project\\flight_transforms",
                target="/usr/app",
                type="bind",
            ),
            Mount(
                source="C:\\Users\\athee\\.dbt",
                target="/root/.dbt",
                type="bind",
            ),
        ],
    )

    # ==========================================
    # Pipeline order
    # ==========================================

    extract_historical_task >> upload_to_s3_task >> run_glue_task >> run_dbt_run_task >> run_dbt_test_task