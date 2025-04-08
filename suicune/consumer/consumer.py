# from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
# from kafka.admin import NewTopic
# import redis
# import time
# import json
# import random

# # Kafka and Redis setup
# bootstrap_servers = 'kafka:9092'
# redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# sync_topic = 'synctopic'
# subscribed_nodes=set()

# # Connect to Redis and register consumer
# def register_consumer():
#     consumer_id = redis_client.incr("consumer_count") - 1
#     redis_client.sadd("active_consumers", f"p{consumer_id}")

#     subscribed_nodes.add(f"p{consumer_id}")
    
#     # Create topics for communication with existing consumers
#     existing_consumers = redis_client.smembers("active_consumers")
#     print(existing_consumers,"\n",flush=True)
#     topics = []
    
#     for other in existing_consumers:
#         if other != f"p{consumer_id}":

#             subscribed_nodes.append(other)

#             topic1 = f"{other}p{consumer_id}"
#             topic2 = f"p{consumer_id}{other}"
#             redis_client.hset("topics", topic1, "1")
#             redis_client.hset("topics", topic2, "1")
#             topics.extend([topic1, topic2])
    
#     return f"p{consumer_id}", topics

# # Cleanup function
# def unregister_consumer(consumer_id):
#     redis_client.srem("active_consumers", consumer_id)
    
#     # Remove topics related to the consumer
#     topic_keys = [key for key in redis_client.hkeys("topics") if consumer_id in key]
#     for topic in topic_keys:
#         redis_client.hdel("topics", topic)

#     if not redis_client.scard("active_consumers"):
#         redis_client.delete("consumer_count")
#         redis_client.delete("topics")

# # Create Kafka topics dynamically
# def create_topics(topics):
#     admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
#     existing_topics = set(admin_client.list_topics())

#     new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics if t not in existing_topics]

#     if new_topics:
#         admin_client.create_topics(new_topics)

# # Message handling
# def message_receiver(msg):
#     global logical_time
#     msg = msg.value
#     received_time = msg["logical_time"]
#     logical_time = max(received_time, logical_time) + 1
#     print(f"Received:\n{msg}\n", flush=True)
#     return msg["process"]

# def message_sender(consumer_id, topic):
#     global logical_time
#     logical_time += 1

#     drift = random.randint(0, 5)
#     physical_time = list(map(int, time.strftime("%H:%M:%S", time.localtime()).split(':')))
#     physical_time[2] += drift
#     if physical_time[2] >= 60:
#         physical_time[2] -= 60
#         physical_time[1] += 1
#     if physical_time[1] >= 60:
#         physical_time[1] -= 60
#         physical_time[0] += 1
#     if physical_time[0] >= 24:
#         physical_time[0] = 0

#     message = {
#         "flag": "1",
#         "process": consumer_id,
#         "logical_time": logical_time,
#         "physical_time": physical_time,
#         "event": f"Hi! I am from {consumer_id}"
#     }

#     producer.send(topic, value=message)
#     producer.flush()
#     print(f"Sending:\n{message}\n", flush=True)

# def synchronize_clock(msg):
#     global physical_time
#     sync_str = msg.value["sync_time"]
#     physical_time = list(map(int, sync_str.split(':')))
#     print(f"Syncing {physical_time}", flush=True)

# try:
#     consumer_id, topics = register_consumer()
#     print(f"Registered Consumer: {consumer_id} | Subscribed Topics: {topics}", flush=True)

#     create_topics(topics)

#     consumer = KafkaConsumer(
#         sync_topic, *topics,
#         bootstrap_servers=bootstrap_servers,
#         auto_offset_reset='earliest',
#         group_id='test-group',
#         enable_auto_commit=False,
#         value_deserializer=lambda v: json.loads(v.decode('utf-8'))
#     )
    
#     producer = KafkaProducer(
#         bootstrap_servers=bootstrap_servers,
#         value_serializer=lambda v: json.dumps(v).encode('utf-8')
#     )

#     logical_time = 0

#     if len(topics) > 0 and consumer_id == "p1":
#         possible_recipients = [p for p in redis_client.smembers("active_consumers") if p != consumer_id]
#         # random_topic = random.choice(topics)
#         if possible_recipients:
#             next_recipient = random.choice(possible_recipients)
#             print(f"sending to topic {consumer_id}{next_recipient}",flush=True)
#             message_sender(consumer_id, f"{consumer_id}{next_recipient}")

#     for msg in consumer:

        
#         # consumer = KafkaConsumer(
#         #     sync_topic, *topics,
#         #     bootstrap_servers=bootstrap_servers,
#         #     auto_offset_reset='earliest',
#         #     group_id='test-group',
#         #     enable_auto_commit=False,
#         #     value_deserializer=lambda v: json.loads(v.decode('utf-8'))
#         # )

#         # print(type(existing_consumers)) # set datatype
#         existing_consumers = redis_client.smembers("active_consumers")
#         # existing_consumers.sort()
#         # print(existing_consumers,flush=True)


#         for i in existing_consumers.symmetric_difference(subscribed_nodes):
#             consumer.subscribe(f"{i}p{consumer_id}")
#             print(i)
#             # consumer.subscribe(f"p{consumer
#             subscribed_nodes.add(i)

#         admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
#         existing_topics = set(admin_client.list_topics())

#         if msg.value["flag"] == "0":
#             synchronize_clock(msg)

#         else:
#             last_sender = message_receiver(msg)

#             time.sleep(4)

#             possible_recipients = [p for p in redis_client.smembers("active_consumers") if p != consumer_id]
#             if possible_recipients:
#                 next_recipient = random.choice(possible_recipients)
#                 message_sender(consumer_id, f"{consumer_id}{next_recipient}")

#         consumer.commit()

# except KeyboardInterrupt:
#     unregister_consumer(consumer_id)
#     consumer.commit()
#     consumer.close()














































from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import redis
import time
import json
import random
import threading

bootstrap_servers = 'kafka:9092'
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

sync_topic = 'synctopic'
update_topic = 'update_topic'

def register_consumer():
    """ Registers the new consumer, updates Redis, and notifies existing consumers """
    consumer_id = redis_client.incr("consumer_count") - 1
    consumer_name = f"p{consumer_id}"

    # Store consumer in Redis
    redis_client.sadd("active_consumers", consumer_name)

    existing_consumers = redis_client.smembers("active_consumers")
    topics = []
    consumer_topics = []

    # Generate new topics only for this consumer
    for other in existing_consumers:
        if other != consumer_name:
            topic1, topic2 = f"{other}{consumer_name}", f"{consumer_name}{other}"
            redis_client.hset("topics", topic1, "1")
            redis_client.hset("topics", topic2, "1")
            topics.extend([topic1, topic2])
            # Ensure only relevant consumers are updated
            redis_client.sadd(f"subscriptions:{other}", topic2)
            redis_client.sadd(f"subscriptions:{consumer_name}", topic1)

    # Notify only affected consumers
    redis_client.publish(update_topic, json.dumps({
        "new_consumer": consumer_name,
        "new_topics": topics
    }))

    return consumer_name, redis_client.smembers(f"subscriptions:{consumer_name}")

def listen_for_updates(consumer_name, consumer):
    """ Listens for new topics and updates consumer subscriptions dynamically """
    pubsub = redis_client.pubsub()
    pubsub.subscribe(update_topic)

    for message in pubsub.listen():
        if message["type"] == "message":
            data = json.loads(message["data"])
            new_consumer = data["new_consumer"]
            new_topics = data["new_topics"]

            if consumer_name != new_consumer:
                relevant_topics = redis_client.smembers(f"subscriptions:{consumer_name}")
                consumer.subscribe(["synctopic"]+list(relevant_topics))
                print(f"{consumer_name} updated subscriptions: {relevant_topics}", flush=True)

def unregister_consumer(consumer_name):
    """ Cleanup consumer data from Redis on termination """
    redis_client.srem("active_consumers", consumer_name)
    redis_client.delete(f"subscriptions:{consumer_name}")
    topic_keys = [key for key in redis_client.hkeys("topics") if consumer_name in key]
    
    for topic in topic_keys:
        redis_client.hdel("topics", topic)

    if not redis_client.scard("active_consumers"):
        redis_client.delete("consumer_count")
        redis_client.delete("topics")

def create_topics(topics):
    """ Creates Kafka topics dynamically """
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    existing_topics = set(admin_client.list_topics())
    new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics if t not in existing_topics]

    if new_topics:
        admin_client.create_topics(new_topics)

def message_receiver(msg):

    global logical_time

    msg = msg.value
    received_time = msg["logical_time"]

    logical_time = max(received_time, logical_time) + 1

    msg["logical_time"]=logical_time
    print(f"Received:\n{msg}\n", flush=True)

    return msg["process"]

def message_sender(consumer_name, topic):

    global logical_time
    logical_time += 1

    drifter()

    message["logical_time"]=logical_time
    message["physical_time"]=physical_time


    producer.send(topic, value=message)
    producer.flush()
    print(f"Sending:\n{message}\n", flush=True)

def synchronize_clock(msg):
    global physical_time

    sync_str = msg.value["sync_time"]
    physical_time = list(map(int, sync_str.split(':')))
    print(f"Syncing {physical_time}", flush=True)

def drifter():
    drift = random.randint(0, 5)

    physical_time[2] += drift
    if physical_time[2] >= 60:
        physical_time[2] -= 60
        physical_time[1] += 1
    if physical_time[1] >= 60:
        physical_time[1] -= 60
        physical_time[0] += 1
    if physical_time[0] >= 24:
        physical_time[0] = 0

try:
    consumer_name, topics = register_consumer()
    print(f"Registered Consumer: {consumer_name} | Subscribed Topics: {topics}",flush=True)

    create_topics(topics)

    consumer = KafkaConsumer(
        sync_topic, *topics,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        group_id='test-group',
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    sync_consumer = KafkaConsumer(
        sync_topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        group_id=None,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    sync_consumer.poll(timeout_ms=1000)

    for partition in sync_consumer.assignment():
        sync_consumer.seek_to_end(partition)

    def listen_to_sync():
        for msg in sync_consumer:
            print("I am a thread")
            synchronize_clock(msg)
            # sync_consumer.commit()


    sync_thread = threading.Thread(target=listen_to_sync, daemon=True)
    sync_thread.start()

    # Global variables
    logical_time = 0
    physical_time = list(map(int, time.strftime("%H:%M:%S", time.localtime()).split(':')))

    message = {
        "flag": "1",
        "process": consumer_name,
        "logical_time": logical_time,
        "physical_time": physical_time,
        "event": f"Hi! I am from {consumer_name}"
    }

    # Start listening for updates in a separate thread
    update_listener_thread = threading.Thread(target=listen_for_updates, args=(consumer_name, consumer), daemon=True)
    update_listener_thread.start()

    print("I am legend",consumer.subscription())
    if len(consumer.subscription())== 2:
        # random_topic = random.choice(list(topics))
        for j in list(consumer.subscription()):
            if j!="synctopic":
                message_sender(consumer_name,j[2:4]+j[0:2])
    print("hi")
    
    for msg in consumer:
        print("da topics",consumer.subscription())
        print("inner hi")
        if msg.value["flag"] == "0":
            synchronize_clock(msg)
        else:
            last_sender = message_receiver(msg)

            time.sleep(4)

            possible_recipients = [p for p in redis_client.smembers("active_consumers") if p != consumer_name]
            if possible_recipients:
                next_recipient = random.choice(possible_recipients)
                print(possible_recipients)
                print(next_recipient)
                message_sender(consumer_name, f"{consumer_name}{next_recipient}")

        consumer.commit()

except KeyboardInterrupt:

    possible_recipients = [p for p in redis_client.smembers("active_consumers") if p != consumer_name]
    if possible_recipients:
        next_recipient = random.choice(possible_recipients)
        print(possible_recipients)
        print(next_recipient)
        message_sender(consumer_name, f"{consumer_name}{next_recipient}")

    unregister_consumer(consumer_name)
    # sync_consumer.commit()
    sync_consumer.close()
    consumer.commit()
    consumer.close()
