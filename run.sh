sudo systemctl start docker
sudo systemctl enable docker

sudo docker network create kafka-bridge
sudo docker-compose up -d

# check topics
sudo docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --list

sudo docker exec -it kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic clkclk

sudo docker-compose up -d

# in ./producer
sudo docker build -t kafka-producer -f Dockerfile-producer .
sudo docker run --rm --network jolt_kafka-bridge --name producer kafka-producer

# in ./consumer
sudo docker build -t kafka-consumer -f Dockerfile-consumer .
sudo docker run --rm --network jolt_kafka-bridge --name consumer kafka-consumer

