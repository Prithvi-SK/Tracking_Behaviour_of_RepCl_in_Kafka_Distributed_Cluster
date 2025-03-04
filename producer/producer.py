# from kafka import KafkaProducer
# import time

# producer = KafkaProducer(bootstrap_servers='kafka:9092')
# topic = 'scale_clk'

# # for i in range(10):
# message = time.strftime("%H:%M:%S",time.localtime())
# producer.send(topic, message.encode('utf-8'))
# print(f'Sent: {message}')
# time.sleep(5)

# producer.close()

from kafka import KafkaProducer, KafkaConsumer
import time
import json
import random

request_topic = 'p1p2'
response_topic = 'p2p1'
sync_topic = 'synctopic'
bootstrap_servers = 'kafka:9092'


producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

current=time.strftime("%H:%M:%S",time.localtime())
message={
    "flag" : "0",
    "sync_time" : current
    }

while True:
    time.sleep(10)
    producer.send(sync_topic,value=message)
    producer.flush()

    print(f"Syncing {message}",flush=True)

