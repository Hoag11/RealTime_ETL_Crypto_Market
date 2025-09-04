```
```shell

mkdir -p logging
sudo chown -R 1001:1001 logging

sudo docker container stop project-spark || true && \
sudo docker container rm project-spark || true && \
sudo docker run -ti --rm --name project-spark \
  --network=streaming-network-ez \
  --dns 8.8.8.8 \
  -p 4040:4040 \
  -v $(pwd):/app \
  -v $(pwd)/logging:/app/logging \
  -v spark_lib:/opt/bitnami/spark/.ivy2 \
  -v spark_data:/data \
  -w /app \
  -e PYSPARK_DRIVER_PYTHON='python' \
  -e PYSPARK_PYTHON='/data/pyspark_venv/bin/python' \
  -e PYTHONPATH=/app \
  -e KAFKA_BOOTSTRAP_SERVERS='localhost:9094,localhost:9194,localhost:9294' \
  -e KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="Unigap@2024";' \
  spark/spark:3.5 bash -c "\
    python -m venv /data/pyspark_venv && \
    source /data/pyspark_venv/bin/activate && \
    pip install --upgrade pip && \
    pip install -r /app/requirements.txt && \
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.6 \
      spark/spark/consumer_price.py"


```
```

```
```shell
sudo docker container stop project-spark || true && \
sudo docker container rm project-spark || true && \
sudo docker run -ti --rm --name project-spark \
  --network=streaming-network-ez \
  -p 4040:4040 \
  -v $(pwd)/spark/spark/consumer_price.py:/app/consumer_price.py \
  -v $(pwd)/configs/configs.py:/app/configs/configs.py \
  -v $(pwd)/requirements.txt:/app/requirements.txt \
  -v $(pwd)/logging:/app/logging \
  -v spark_lib:/opt/bitnami/spark/.ivy2 \
  -v spark_data:/data \
  -w /app \
  -e PYSPARK_DRIVER_PYTHON=python \
  -e PYSPARK_PYTHON=/data/pyspark_venv/bin/python \
  -e PYTHONPATH=/app \
  spark/spark:3.5 bash -c "\
    python -m venv /data/pyspark_venv && \
    source /data/pyspark_venv/bin/activate && \
    pip install --upgrade pip && \
    pip install -r /app/requirements.txt && \
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.6 \
      /app/consumer_price.py"

```
```

