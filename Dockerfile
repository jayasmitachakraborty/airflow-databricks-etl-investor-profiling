# syntax=docker/dockerfile:1
FROM apache/airflow:2.8.4-python3.11

COPY --chown=airflow:root requirements-docker.txt /opt/airflow/requirements-docker.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements-docker.txt

ENV PYTHONPATH=/opt/airflow/include
