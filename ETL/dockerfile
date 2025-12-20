FROM apache/airflow:3.0.6

USER root

# Base packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    build-essential \
    openjdk-17-jdk \
 && rm -rf /var/lib/apt/lists/*

# Install Docker CLI (official, mới)
RUN curl -fsSL https://get.docker.com | sh

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

COPY requirements_airflow.txt .
RUN pip install --no-cache-dir -r requirements_airflow.txt
