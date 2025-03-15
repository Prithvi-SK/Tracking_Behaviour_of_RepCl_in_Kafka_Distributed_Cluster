docker exec -it kafka kafka-topics --delete --bootstrap-server kafka:9092 --topic synctopic
docker exec -it kafka kafka-topics --delete --bootstrap-server kafka:9092 --topic p1p2
docker exec -it kafka kafka-topics --delete --bootstrap-server kafka:9092 --topic p2p1

docker exec -it kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic synctopic
docker exec -it kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic p1p2
docker exec -it kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic p2p1