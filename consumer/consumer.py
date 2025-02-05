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
import json

request_topic = 'requesttopic'
response_topic = 'responsetopic'
bootstrap_servers = 'kafka:9092'

consumer = KafkaConsumer(request_topic, 
                         bootstrap_servers=bootstrap_servers, 
                         auto_offset_reset='earliest', 
                         group_id='test-group', 
                         enable_auto_commit=True,
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for msg in consumer:
    # received_message = msg.value.decode()
    print(f"raw {msg.value}")
    # try:
    #     data = json.loads(msg.value.decode('utf-8'))
    #     print(f"Received: {data}")
    # except json.JSONDecodeError as e:
    #     print(f"Skipping corrupted message: {e}")
    # recieved_time=msg.value
    # print(f"Received: {recieved_time}")

    # Process and send response
    # response_message = f"Processed message: {received_message}"
    message = time.strftime("%H:%M:%S",time.localtime())
    message={"process":5}
    producer.send(response_topic, value=message)
    producer.flush()
    print(f"Sent: {message}")
    break  # Stop after one request-response cycle
