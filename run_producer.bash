sudo docker run -ti --rm --name project-producer \
  --network=streaming-network-ez \
  -e PYTHONPATH=/app \
  project-producer
