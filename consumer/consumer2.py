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
import random

request_topic = 'p1p2'
response_topic = 'p2p1'
sync_topic='synctopic'
bootstrap_servers = 'kafka:9092'

consumer = KafkaConsumer(sync_topic, request_topic,
                         bootstrap_servers=bootstrap_servers, 
                         auto_offset_reset='earliest', 
                         group_id='test-group', 
                         enable_auto_commit=False,
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

logical_time=0
received_time=0
physical_time=[11,59,55]

message={
    "flag" : "1",
    "process":2,
    "logical_time":logical_time,
    "physical_time":physical_time,
    "event":""
    }


def drifter():
    global physical_time
    drift=random.randint(0,5)
    
    # drift is always in seconds
    if physical_time[2]+drift>=60:
        physical_time[2]=physical_time[2]+drift-60
        if physical_time[1]+1==60:
            physical_time[1]=0
            if physical_time[0]+1==24:
                physical_time[0]=0
            else:
                physical_time[0]=physical_time[0]+1
        else:
            physical_time[1]=physical_time[1]+1
    else:
        physical_time[2]=physical_time[2]+drift


def receiver(msg):
    global message, logical_time, physical_time, received_time

    msg=msg.value
    received_time=msg["logical_time"]
    print(received_time,logical_time,flush=True)
    logical_time=max(received_time, logical_time)+1
    msg["logical_time"]=logical_time
    message["logical_time"]=logical_time
    

    print(f"Received\n{msg}\n",flush=True)


def sender():
    global message, logical_time, physical_time

    logical_time=logical_time+1

    drifter()
    message["event"]="Hi! I am from p2"
    message["logical_time"]=logical_time
    message["physical_time"]=physical_time

    producer.send(response_topic,value=message)
    producer.flush()

    print(f"Sending\n{message}\n",flush=True)


def synchronizer(msg):
    global physical_time

    msg=msg.value
    sync_str=msg["sync_time"]
    synchronized_time=sync_str.split(':')
    synchronized_time=[int(i) for i in synchronized_time]
    physical_time=synchronized_time
    print(f"syncing {physical_time}", flush=True)

try:
    for msg in consumer:
        # print(msg.value)
        if msg.value["flag"]=="0":
            print("AAAAAAAAAAAAAAAAAAAAAAAAAAAA",flush=True)
            synchronizer(msg)
        else:
            receiver(msg)
            # time.sleep(1)
            sender()
            # time.sleep(5)
        
        consumer.commit()
            # break  

except KeyboardInterrupt:
    consumer.commit()
    consumer.close()
