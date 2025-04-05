from kafka import KafkaProducer, KafkaConsumer
import time
import json
import random

# no_of_cons=int(input("Enter no. of nodes: "))

# topics=[]
# for i in range(no_of_cons):
#     row=[]
#     for j in range(no_of_cons):
#         row.append(f"p{i}p{j}")
#     topics.append(row)
# print(topics)

# d={}
# for i in range(no_of_cons):
#     l=[]
#     for j in range(no_of_cons):
#         l.append(f"p{i}p{j}")

bootstrap_servers = 'kafka:9092'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8')
                         )
consumer = KafkaConsumer(sync_topic, response_topic, 
                         bootstrap_servers=bootstrap_servers, 
                         auto_offset_reset='earliest', 
                         group_id='test-group', 
                         enable_auto_commit=False,
                         value_deserializer=lambda v: json.loads(v.decode('utf-8'))
                         )