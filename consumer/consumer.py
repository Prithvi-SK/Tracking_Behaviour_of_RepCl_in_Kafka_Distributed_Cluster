# from kafka import KafkaConsumer
# import random

# consumer = KafkaConsumer(
#     'scale_clk',
#     bootstrap_servers='kafka:9092',
#     auto_offset_reset='earliest',
#     group_id='test-group'
# )

# def rantime():
#     h=str(random.randint(0,23))
#     m=str(random.randint(0,59))
#     s=str(random.randint(0,59))
#     return h+":"+m+":"+s

# for message in consumer:
#     while True:
#         r=rantime()
#         if r!=message.value.decode('utf-8'):
#             print(f"Received: {r}")
#             break
#     break

# kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic clkclk

from kafka import KafkaConsumer, KafkaProducer
import time

request_topic = 'requesttopic'
response_topic = 'responsetopic'
bootstrap_servers = 'kafka:9092'

consumer = KafkaConsumer(request_topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest', group_id='test-group')
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

for msg in consumer:
    # received_message = msg.value.decode()
    print(f"Received: {msg.value.decode()}")

    # Process and send response
    # response_message = f"Processed message: {received_message}"
    message = time.strftime("%H:%M:%S",time.localtime())
    producer.send(response_topic, message.encode())
    producer.flush()
    print(f"Sent: {message}")
    break  # Stop after one request-response cycle
