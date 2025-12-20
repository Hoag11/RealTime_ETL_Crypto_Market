#!/bin/bash
# ===============================================
# Spark Streaming Job Runner
# Stop old container & run new streaming Spark job
# ===============================================

CONTAINER_NAME="stream-loader-ez"
NETWORK_NAME="streaming-network-ez"
SPARK_IMAGE="spark/spark:3.5"
SPARK_SCRIPT="/spark/spark/load/streaming_load.py"
JOB_NAME="fact_coin_metrics_streaming_job"

# Dừng container cũ (nếu có)
echo "[INFO] Dừng container cũ nếu đang chạy..."
docker container stop "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker container rm "$CONTAINER_NAME" >/dev/null 2>&1 || true

# Chờ container cũ xoá xong
while [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; do
  echo "[INFO] Đang chờ container cũ được xoá..."
  sleep 1
done

# Chạy container Spark Streaming mới
echo "[INFO] Khởi chạy Spark Structured Streaming job..."
docker run --rm --name "$CONTAINER_NAME" \
  --network "$NETWORK_NAME" \
  -p 4042:4040 \
  -v /home/hoang/Projects/RealTime_ETL_Crypto_Market/ETL/spark:/spark \
  -v /home/hoang/Projects/RealTime_ETL_Crypto_Market/ETL/configs:/configs \
  -v spark_lib:/opt/bitnami/spark/.ivy2 \
  -v spark_data:/data \
  "$SPARK_IMAGE" bash -c "
    pip install psycopg2-binary > /dev/null 2>&1
    export PYTHONPATH=\$PYTHONPATH:/configs &&
    spark-submit \
      --packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.6 \
      --executor-memory 1g --driver-memory 1g \
      --name $JOB_NAME \
      $SPARK_SCRIPT
  "

# Kết thúc
if [ $? -eq 0 ]; then
  echo "[SUCCESS] Spark streaming job đã hoàn tất."
else
  echo "[ERROR] Spark streaming job thất bại."
fi
