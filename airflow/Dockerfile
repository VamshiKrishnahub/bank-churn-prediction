FROM apache/airflow:2.7.3

USER airflow

RUN pip install --user --no-cache-dir \
    great_expectations==0.18.12 \
    pandas==1.5.3 \
    sqlalchemy==1.4.52 \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-postgres==5.7.1
