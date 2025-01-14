from kafka import KafkaConsumer
import random

consumer = KafkaConsumer(
    'clkclk',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    group_id='test-group'
)

def rantime():
    h=str(random.randint(0,23))
    m=str(random.randint(0,59))
    s=str(random.randint(0,59))
    return h+":"+m+":"+s

for message in consumer:
    while True:
        r=rantime()
        if r!=message.value.decode('utf-8'):
            print(f"Received: {r}")
            break
    break

# kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic clkclk