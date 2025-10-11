FROM apache/airflow:3.0.6 

# Install custom Python dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential  # Optional, but good for any C-based packages
USER airflow

COPY requirements_airflow.txt .
RUN pip install --no-cache-dir -r requirements_airflow.txt
