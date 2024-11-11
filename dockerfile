FROM apache/airflow:latest

USER root

RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean


USER airflow

# Install pyspark
RUN pip install pyspark

RUN pip install apache-airflow

