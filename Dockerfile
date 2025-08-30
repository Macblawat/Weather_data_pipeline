FROM python:3.10.12


RUN apt-get update && \
    apt-get install -y gcc g++ unixodbc-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV AIRFLOW_HOME=/app/airflow


COPY requirements.txt .

ARG AIRFLOW_VERSION=2.7.3
ARG PYTHON_VERSION=3.10
RUN pip install "apache-airflow[postgres,azure]==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"


RUN pip install --no-cache-dir -r requirements.txt

COPY . .


RUN mkdir -p $AIRFLOW_HOME/dags


COPY *.py $AIRFLOW_HOME/dags/


COPY start_airflow.sh .
RUN chmod +x start_airflow.sh


EXPOSE 8080


CMD ["./start_airflow.sh"]