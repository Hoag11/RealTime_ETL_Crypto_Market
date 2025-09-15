sudo docker build -t project-consumer -f Dockerfile.consumer .
sudo docker run -ti --rm --name project-consumer \
  -v /logging:/app/logging \
  --network=streaming-network-ez \
  -p 4040:4040 \
  project-consumer
