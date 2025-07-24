Simulates a distributed cluster with clock metrics in microsecond to analyze the behavior of Replay Clocks against race conditions. Nodes in the cluster are asynchronous, fault tolerant and scalable.
  
## Producer

Periodically sends the current timestamp in microseconds to the topic- ‘sync\_topic’

There are 2 producers to handle fault tolerance. If one fails the other takes over.

## Consumer  
Each consumer communicates with all other consumers in the cluster. It performs the following  

1.  Multi-threading
    1.  Thread 1 - Listens to synctopic
    2.  Thread 2 - Get and Post to Redis DB
    3.  Remaining threads - Asynchronously send messages.
2.  Scalable
    1.  Consumers can be scaled to any number and the updates are made to Redis so that the remaining consumers can subscribe to the new topics.
3.  Events
    1.  Send - Updates its RepCl before sending the message and writes to logs
    2.  Receive - Updates its RepCl after receiving the message and writes to logs

## Kafka  
Acts as a message broker between producer and consumer.

## Redis

Stores information about available topics, active consumers and keeps updating the database as changes happen in the system

## Docker

Producers and consumers run in new containers and communicate over bridge network. Logs are written to a volume.  

## Message Format Example

{'flag': '1', 'source': '2', 'destination': 0, 'logical\_time': 896, 'physical\_time': 6403068, 'event': 'send', 'repcl': {'mx': 599, 'bitmap': \[0, 0, 1, 0, 0, 0, 0, 0, 0, 0\], 'offsets': \[1000000, 1000000, 535, 1000000, 1000000, 1000000, 1000000, 1000000, 1000000, 1000000\], 'counters': \[0, 0, 1, 0, 0, 0, 0, 0, 0, 0\]}}

## Languages

*   Python

## Tools

*   Docker
*   Kafka
*   Redis
