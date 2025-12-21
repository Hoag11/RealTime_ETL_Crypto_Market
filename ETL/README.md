# RealTime_ETL_Crypto_Market

```bash
cd ETL
docker netowrk create streaming-network-ez
```
** airflow
```bash
docker compose build
```
hoặc 
```bash
docker-compose build
```
sau đó chạy
```bash
docker compose up -d
```
hoặc 
```
docker-compose up -d
```
** spark
```bash
cd spark
```
```bash
docker build -t spark/spark:3.5 .
```
```
docker volume create spark-lib
```
```
docker volume create spark-data
```
```
docker compose up -d
```

** Minio. Kafka, Postgres
cd vào docker/{thư mục tương ứng}
```
docker compose up -d
```
