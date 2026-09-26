FROM apache/airflow:2.9.3

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==2.9.3" \
    -r /requirements.txt