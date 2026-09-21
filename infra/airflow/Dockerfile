FROM apache/spark:3.5.3-python3 AS spark-runtime

FROM apache/airflow:2.10.3-python3.11

USER root
COPY --from=spark-runtime /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless python3-venv curl && rm -rf /var/lib/apt/lists/* \
 && curl --fail --location --retry 3 --output /opt/airflow/delta-spark_2.12-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
 && curl --fail --location --retry 3 --output /opt/airflow/delta-storage-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
 && python -m venv /opt/dbt-venv \
 && chown -R airflow:root /opt/dbt-venv
COPY infra/airflow/requirements.txt /requirements.txt
COPY infra/airflow/dbt-requirements.txt /dbt-requirements.txt
USER airflow
RUN pip install --no-cache-dir --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt" -r /requirements.txt \
 && pip install --no-cache-dir --no-deps delta-spark==3.2.0 \
 && /opt/dbt-venv/bin/pip install --no-cache-dir -r /dbt-requirements.txt