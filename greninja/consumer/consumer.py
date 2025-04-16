
# from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
# from kafka.admin import NewTopic
# import redis
# import time
# import json
# import random
# import threading

# bootstrap_servers = 'kafka:9092'
# redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# sync_topic = 'synctopic'
# update_topic = 'update_topic'

# stop_event = threading.Event()


# def register_consumer():
#     """ Registers the new consumer, updates Redis, and notifies existing consumers """
#     consumer_id = redis_client.incr("consumer_count") - 1
#     consumer_name = f"p{consumer_id}"

#     # Store consumer in Redis
#     redis_client.sadd("active_consumers", consumer_name)

#     existing_consumers = redis_client.smembers("active_consumers")
#     topics = []
#     consumer_topics = []

#     # Generate new topics only for this consumer
#     for other in existing_consumers:
#         if other != consumer_name:
#             topic1, topic2 = f"{other}{consumer_name}", f"{consumer_name}{other}"
#             redis_client.hset("topics", topic1, "1")
#             redis_client.hset("topics", topic2, "1")
#             topics.extend([topic1, topic2])
#             # Ensure only relevant consumers are updated
#             redis_client.sadd(f"subscriptions:{other}", topic2)
#             redis_client.sadd(f"subscriptions:{consumer_name}", topic1)

#     # Notify only affected consumers
#     redis_client.publish(update_topic, json.dumps({
#         "new_consumer": consumer_name,
#         "new_topics": topics
#     }))

#     return consumer_name, redis_client.smembers(f"subscriptions:{consumer_name}")

# def listen_for_updates(consumer_name, consumer):
#     """ Listens for new topics and updates consumer subscriptions dynamically """
#     pubsub = redis_client.pubsub()
#     pubsub.subscribe(update_topic)

#     for message in pubsub.listen():
#         if message["type"] == "message":
#             data = json.loads(message["data"])
#             new_consumer = data["new_consumer"]
#             new_topics = data["new_topics"]

#             if consumer_name != new_consumer:
#                 relevant_topics = redis_client.smembers(f"subscriptions:{consumer_name}")
#                 consumer.subscribe(["synctopic"]+list(relevant_topics))
#                 print(f"{consumer_name} updated subscriptions: {relevant_topics}", flush=True)

# def unregister_consumer(consumer_name):
#     """ Cleanup consumer data from Redis on termination """
#     redis_client.srem("active_consumers", consumer_name)
#     redis_client.delete(f"subscriptions:{consumer_name}")
#     topic_keys = [key for key in redis_client.hkeys("topics") if consumer_name in key]
    
#     for topic in topic_keys:
#         redis_client.hdel("topics", topic)

#     if not redis_client.scard("active_consumers"):
#         redis_client.delete("consumer_count")
#         redis_client.delete("topics")

# def create_topics(topics):
#     """ Creates Kafka topics dynamically """
#     admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
#     existing_topics = set(admin_client.list_topics())
#     new_topics = [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics if t not in existing_topics]

#     if new_topics:
#         admin_client.create_topics(new_topics)

# def message_receiver(msg):

#     global logical_time

#     msg = msg.value
#     received_time = msg["logical_time"]

#     logical_time = max(received_time, logical_time) + 1

#     msg["logical_time"]=logical_time
#     print(f"Received:\n{msg}\n", flush=True)

#     return msg["process"]

# def message_sender(consumer_name, topic):

#     global logical_time
#     logical_time += 1

#     drifter()

#     message["logical_time"]=logical_time
#     message["physical_time"]=physical_time


#     producer.send(topic, value=message)
#     producer.flush()
#     print(f"Sending:\n{message}\n", flush=True)

# def synchronize_clock(msg):
#     global physical_time

#     sync_str = msg.value["sync_time"]
#     physical_time = list(map(int, sync_str.split(':')))
#     print(f"Syncing {physical_time}", flush=True)

# def drifter():
#     drift = random.randint(0, 5)

#     physical_time[2] += drift
#     if physical_time[2] >= 60:
#         physical_time[2] -= 60
#         physical_time[1] += 1
#     if physical_time[1] >= 60:
#         physical_time[1] -= 60
#         physical_time[0] += 1
#     if physical_time[0] >= 24:
#         physical_time[0] = 0



# try:
#     consumer_name, topics = register_consumer()
#     print(f"Registered Consumer: {consumer_name} | Subscribed Topics: {topics}",flush=True)

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

#     sync_consumer = KafkaConsumer(
#         sync_topic,
#         bootstrap_servers=bootstrap_servers,
#         auto_offset_reset='earliest',
#         group_id=None,
#         enable_auto_commit=False,
#         value_deserializer=lambda v: json.loads(v.decode('utf-8'))
#     )
#     sync_consumer.poll(timeout_ms=1000)

#     for partition in sync_consumer.assignment():
#         sync_consumer.seek_to_end(partition)

#     # def listen_to_sync():
#     #     for msg in sync_consumer:
#     #         print("I am a thread")
#     #         synchronize_clock(msg)
#     #         # sync_consumer.commit()
#     def listen_to_sync():
#         while not stop_event.is_set():
#             try:
#                 for msg in sync_consumer:
#                     if stop_event.is_set():
#                         break
#                     print("I am a thread")
#                     synchronize_clock(msg)
#             except Exception as e:
#                 if not stop_event.is_set():
#                     print(f"Sync thread error: {e}", flush=True)
#                 break



#     sync_thread = threading.Thread(target=listen_to_sync, daemon=True)
#     sync_thread.start()

#     # Global variables
#     logical_time = 0
#     physical_time = list(map(int, time.strftime("%H:%M:%S", time.localtime()).split(':')))

#     message = {
#         "flag": "1",
#         "process": consumer_name,
#         "logical_time": logical_time,
#         "physical_time": physical_time,
#         "event": f"Hi! I am from {consumer_name}"
#     }

#     # Start listening for updates in a separate thread
#     update_listener_thread = threading.Thread(target=listen_for_updates, args=(consumer_name, consumer), daemon=True)
#     update_listener_thread.start()

#     print("I am legend",consumer.subscription())
#     if len(consumer.subscription())== 2:
#         # random_topic = random.choice(list(topics))
#         for j in list(consumer.subscription()):
#             if j!="synctopic":
#                 message_sender(consumer_name,j[2:4]+j[0:2])
#     print("hi")
    
#     for msg in consumer:
#         print("da topics",consumer.subscription())
#         print("inner hi")
#         if msg.value["flag"] == "0":
#             # synchronize_clock(msg)
#             continue
#         else:
#             last_sender = message_receiver(msg)

#             time.sleep(4)

#             possible_recipients = [p for p in redis_client.smembers("active_consumers") if p != consumer_name]
#             if possible_recipients:
#                 next_recipient = random.choice(possible_recipients)
#                 print(possible_recipients)
#                 print(next_recipient)
#                 message_sender(consumer_name, f"{consumer_name}{next_recipient}")

#         consumer.commit()

# except KeyboardInterrupt:

#     possible_recipients = [p for p in redis_client.smembers("active_consumers") if p != consumer_name]
#     if possible_recipients:
#         next_recipient = random.choice(possible_recipients)
#         print(possible_recipients)
#         print(next_recipient)
#         message_sender(consumer_name, f"{consumer_name}{next_recipient}")

#     unregister_consumer(consumer_name)

#     stop_event.set()               # Signal the thread to stop
#     sync_thread.join(timeout=2)    # Wait for thread to finish

#     # sync_consumer.commit()
#     sync_consumer.close()
#     consumer.commit()
#     consumer.close()




































from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import redis
import time
import json
import random
import threading
import uuid

bootstrap_servers = 'kafka:9092'
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
sync_topic = 'synctopic'
update_topic = 'update_topic'
stop_event = threading.Event()

def generate_uuid():
    return str(uuid.uuid4())

def register_consumer():
    """Registers a new consumer and updates Redis."""
    consumer_id = redis_client.incr("consumer_count")-1
    consumer_name = f"p{consumer_id}"
    redis_client.sadd("active_consumers", consumer_name)

    topics = []
    existing_consumers = redis_client.smembers("active_consumers")
    for other in existing_consumers:
        if other != consumer_name:
            topic_out = f"{consumer_name}{other}"
            topic_in = f"{other}{consumer_name}"
            redis_client.hset("topics", topic_out, "1")
            redis_client.hset("topics", topic_in, "1")
            topics.append(topic_in)
            redis_client.sadd(f"subscriptions:{consumer_name}", topic_in)
            redis_client.sadd(f"subscriptions:{other}", topic_out)

    if topics:
        redis_client.publish(update_topic, json.dumps({
            "new_consumer": consumer_name,
            "new_topics": topics
        }))

    return consumer_name, topics

def listen_for_updates(consumer_name, consumer):
    """Listens for new topics and updates consumer subscriptions."""
    pubsub = redis_client.pubsub()
    pubsub.subscribe(update_topic)
    for message in pubsub.listen():
        if message["type"] == "message":
            data = json.loads(message["data"])
            if data["new_consumer"] != consumer_name:
                relevant_topics = redis_client.smembers(f"subscriptions:{consumer_name}")
                consumer.subscribe([sync_topic] + list(relevant_topics))
                print(f"{consumer_name} updated subscriptions: {relevant_topics}", flush=True)

def unregister_consumer(consumer_name):
    """Cleans up consumer data from Redis."""
    redis_client.srem("active_consumers", consumer_name)
    redis_client.delete(f"subscriptions:{consumer_name}")
    for topic in redis_client.hkeys("topics"):
        if consumer_name in topic:
            redis_client.hdel("topics", topic)
    if not redis_client.scard("active_consumers"):
        redis_client.delete("consumer_count", "topics")

def create_topics(topics):
    """Creates Kafka topics dynamically."""
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    existing_topics = admin_client.list_topics()
    new_topics = [
        NewTopic(name=t, num_partitions=1, replication_factor=1)
        for t in topics if t not in existing_topics
    ]
    if new_topics:
        admin_client.create_topics(new_topics)
    admin_client.close()

# receive, print, updatelogical, updatelogical, print
def message_receiver(msg, logical_time):
    """Processes received messages and updates logical time."""
    msg = msg.value
    logical_time = max(msg["logical_time"], logical_time) + 1
    msg["logical_time"]=logical_time
    print(f"Received:\n{msg}\n", flush=True)
    return msg["process"], logical_time

def message_sender(consumer_name, topic, message, logical_time, physical_time, producer):
    """Sends messages with updated logical and physical times."""
    logical_time += 1
    physical_time = update_physical_time(physical_time)
    message.update({
        "logical_time": logical_time,
        "physical_time": physical_time
    })
    producer.send(topic, value=message)
    producer.flush()
    print(f"Sending:\n{message}\n", flush=True)
    return logical_time

def synchronize_clock(msg, physical_time):
    """Synchronizes physical time from sync messages."""
    sync_str = msg.value["sync_time"]
    physical_time[:] = list(map(int, sync_str.split(':')))
    print(f"Syncing {physical_time}", flush=True)
    return physical_time

def update_physical_time(physical_time):
    """Simulates clock drift."""
    drift = random.randint(0, 5)
    physical_time[2] += drift
    while physical_time[2] >= 60:
        physical_time[2] -= 60
        physical_time[1] += 1
    while physical_time[1] >= 60:
        physical_time[1] -= 60
        physical_time[0] += 1
    physical_time[0] %= 24
    return physical_time

def main():
    consumer_name, topics = register_consumer()


    create_topics(topics)

    consumer = KafkaConsumer(
        sync_topic, *topics,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        group_id=f'group-{consumer_name}-{generate_uuid()}',
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
        auto_offset_reset='latest',
        group_id=None,
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

    sync_consumer.poll(timeout_ms=1000)

    for partition in sync_consumer.assignment():
        sync_consumer.seek_to_end(partition)

    print(f"Registered Consumer: {consumer_name} | Subscribed Topics: {consumer.subscription()}", flush=True)

    logical_time = 0
    physical_time = list(map(int, time.strftime("%H:%M:%S").split(':')))
    message = {
        "flag": "1",
        "process": consumer_name,
        "logical_time": logical_time,
        "physical_time": physical_time,
        "event": f"Hi! I am from {consumer_name}"
    }

    def listen_to_sync():
        while not stop_event.is_set():
            try:
                messages = sync_consumer.poll(timeout_ms=1000)
                for _, msg_list in messages.items():
                    for msg in msg_list:
                        if msg.value["flag"] == "0":
                            synchronize_clock(msg, physical_time)
            except Exception as e:
                if not stop_event.is_set():
                    print(f"Sync thread error: {e}", flush=True)

    sync_thread = threading.Thread(target=listen_to_sync, daemon=True)
    sync_thread.start()
    update_listener_thread = threading.Thread(target=listen_for_updates, args=(consumer_name, consumer), daemon=True)
    update_listener_thread.start()

    # Initiate communication if joining with other consumers
    # active_consumers = redis_client.smembers("active_consumers")
    # if len(active_consumers) > 1:
    #     recipient = random.choice([p for p in active_consumers if p != consumer_name])
    #     topic = f"{consumer_name}{recipient}"
    #     logical_time = message_sender(consumer_name, topic, message.copy(), logical_time, physical_time, producer)
  
  
    if len(consumer.subscription())== 2:
        # random_topic = random.choice(list(topics))
        for j in list(consumer.subscription()):
            if j!="synctopic":
                # message_sender(consumer_name,j[2:4]+j[0:2])
                logical_time = message_sender(consumer_name, j[2:4]+j[0:2], message.copy(), logical_time, physical_time, producer)
  
  
    try:
        for msg in consumer:
            if msg.value["flag"] == "0":
                continue
            exception_level=1
            sender, logical_time = message_receiver(msg, logical_time)
            exception_level=2
            time.sleep(3)  # Reduced delay for responsiveness
            possible_recipients = [p for p in redis_client.smembers("active_consumers") if p != consumer_name]
            if possible_recipients:
                next_recipient = random.choice(possible_recipients)
                topic = f"{consumer_name}{next_recipient}"

                logical_time = message_sender(consumer_name, topic, message.copy(), logical_time, physical_time, producer)
            
            exception_level=4
            consumer.commit()


    except KeyboardInterrupt:
        if exception_level==1:
            sender, logical_time = message_receiver(msg, logical_time)
            possible_recipients = [p for p in redis_client.smembers("active_consumers") if p != consumer_name]
            if possible_recipients:
                next_recipient = random.choice(possible_recipients)
                topic = f"{consumer_name}{next_recipient}"

                logical_time = message_sender(consumer_name, topic, message.copy(), logical_time, physical_time, producer)
        elif exception_level==2:
            possible_recipients = [p for p in redis_client.smembers("active_consumers") if p != consumer_name]
            if possible_recipients:
                next_recipient = random.choice(possible_recipients)
                topic = f"{consumer_name}{next_recipient}"

                logical_time = message_sender(consumer_name, topic, message.copy(), logical_time, physical_time, producer)
            # next_recipient = random.choice(possible_recipients)
            # topic = f"{consumer_name}{next_recipient}"
            # message_sender(consumer_name, topic, message.copy(), logical_time, physical_time, producer)

    finally:
        unregister_consumer(consumer_name)
        stop_event.set()
        sync_thread.join(timeout=2)
        sync_consumer.close()
        consumer.commit()
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()