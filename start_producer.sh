sudo docker build -t project-producer -f Dockerfile.project-producer .
sudo docker run -ti --rm --name project-producer \
  -v /logging:/app/logging \
  --network=streaming-network-ez \
  -e PYTHONPATH=/app \
  project-producer
