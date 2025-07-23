
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



































# # Works but repcl is dict
# from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
# from kafka.admin import NewTopic
# import redis
# import time
# import json
# import random
# import threading
# import uuid

# bootstrap_servers = 'kafka:9092'
# redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
# sync_topic = 'synctopic'
# update_topic = 'update_topic'
# stop_event = threading.Event()
# time_lock = threading.Lock()
# I = 1  # Epoch size in seconds
# EPSILON = 5  # Maximum clock skew in epochs

# class RepCl:
#     def __init__(self, mx, offsets, counters):
#         self.mx = mx
#         self.offsets = offsets  # dict {int: int}, offset.k < EPSILON
#         self.counters = counters  # dict {int: int}

#     def to_dict(self):
#         return {
#             "mx": self.mx,
#             "offsets": {str(k): v for k, v in self.offsets.items()},
#             "counters": {str(k): v for k, v in self.counters.items()}
#         }

#     @classmethod
#     def from_dict(cls, d):
#         mx = d["mx"]
#         offsets = {int(k): v for k, v in d["offsets"].items()}
#         counters = {int(k): v for k, v in d["counters"].items()}
#         return cls(mx, offsets, counters)

#     def shift(self, newmx, epsilon):
#         delta = newmx - self.mx
#         new_offsets = {}
#         for k, offset in self.offsets.items():
#             new_offset = offset + delta
#             if new_offset < epsilon:
#                 new_offsets[k] = new_offset
#         return RepCl(newmx, new_offsets, self.counters.copy())

#     def merge_same_epoch(self, other, epsilon):
#         ts = RepCl(self.mx, {}, {})
#         all_keys = set(self.offsets.keys()) | set(other.offsets.keys())
#         for k in all_keys:
#             min_offset = min(self.offsets.get(k, epsilon), other.offsets.get(k, epsilon))
#             if min_offset < epsilon:
#                 ts.offsets[k] = min_offset
#         return ts

#     def equal_offset(self, other, epsilon):
#         if self.mx != other.mx:
#             return False
#         keys1 = set(self.offsets.keys())
#         keys2 = set(other.offsets.keys())
#         for k in keys1 | keys2:
#             if self.offsets.get(k, epsilon) != other.offsets.get(k, epsilon):
#                 return False
#         return True

# def update_repcl_send(repcl, pt, process_id, epsilon, i):
#     epoch = int(pt / i)
#     newmx = max(repcl.mx, epoch)
#     new_offset = newmx - epoch
#     if repcl.mx == newmx and repcl.offsets.get(process_id, epsilon) == new_offset:
#         new_repcl = RepCl(repcl.mx, repcl.offsets.copy(), repcl.counters.copy())
#         new_repcl.counters[process_id] = new_repcl.counters.get(process_id, 0) + 1
#     else:
#         shifted_repcl = repcl.shift(newmx, epsilon)
#         new_offsets = shifted_repcl.offsets.copy()
#         if new_offset < epsilon:
#             new_offsets[process_id] = new_offset
#         else:
#             new_offsets.pop(process_id, None)
#         new_repcl = RepCl(newmx, new_offsets, {})
#     return new_repcl

# def update_repcl_receive(repcl_j, repcl_m, pt, process_id, epsilon, i):
#     epoch = int(pt / i)
#     newmx = max(repcl_j.mx, repcl_m.mx, epoch)
#     ts_a = repcl_j.shift(newmx, epsilon)
#     ts_b = repcl_m.shift(newmx, epsilon)
#     ts_c = ts_a.merge_same_epoch(ts_b, epsilon)
#     if repcl_j.equal_offset(ts_c, epsilon) and repcl_m.equal_offset(ts_c, epsilon):
#         all_keys = set(repcl_j.counters.keys()) | set(repcl_m.counters.keys())
#         ts_c.counters = {k: max(repcl_j.counters.get(k, 0), repcl_m.counters.get(k, 0)) for k in all_keys}
#         ts_c.counters[process_id] = ts_c.counters.get(process_id, 0) + 1
#     elif repcl_j.equal_offset(ts_c, epsilon):
#         ts_c.counters = repcl_j.counters.copy()
#         ts_c.counters[process_id] = ts_c.counters.get(process_id, 0) + 1
#     elif repcl_m.equal_offset(ts_c, epsilon):
#         ts_c.counters = repcl_m.counters.copy()
#         ts_c.counters[process_id] = ts_c.counters.get(process_id, 0) + 1
#     else:
#         ts_c.counters = {}
#     return ts_c

# def time_to_seconds(time_list):
#     H, M, S = time_list
#     return H * 3600 + M * 60 + S

# def generate_uuid():
#     return str(uuid.uuid4())

# def register_consumer():
#     consumer_id = redis_client.incr("consumer_count") - 1
#     consumer_name = str(consumer_id)
#     redis_client.sadd("active_consumers", consumer_name)
#     topics = []
#     existing_consumers = redis_client.smembers("active_consumers")
#     for other in existing_consumers:
#         if other != consumer_name:
#             topic_out = f"{consumer_name}{other}"
#             topic_in = f"{other}{consumer_name}"
#             redis_client.hset("topics", topic_out, "1")
#             redis_client.hset("topics", topic_in, "1")
#             topics.append(topic_in)
#             redis_client.sadd(f"subscriptions:{consumer_name}", topic_in)
#             redis_client.sadd(f"subscriptions:{other}", topic_out)
#     if topics:
#         redis_client.publish(update_topic, json.dumps({
#             "new_consumer": consumer_name,
#             "new_topics": topics
#         }))
#     return consumer_name, topics

# def listen_for_updates(consumer_name, consumer):
#     pubsub = redis_client.pubsub()
#     pubsub.subscribe(update_topic)
#     for message in pubsub.listen():
#         if message["type"] == "message":
#             data = json.loads(message["data"])
#             if data["new_consumer"] != consumer_name:
#                 relevant_topics = redis_client.smembers(f"subscriptions:{consumer_name}")
#                 consumer.subscribe([sync_topic] + list(relevant_topics))
#                 print(f"{consumer_name} updated subscriptions: {relevant_topics}", flush=True)

# def unregister_consumer(consumer_name):
#     redis_client.srem("active_consumers", consumer_name)
#     redis_client.delete(f"subscriptions:{consumer_name}")
#     for topic in redis_client.hkeys("topics"):
#         if consumer_name in topic:
#             redis_client.hdel("topics", topic)
#     if not redis_client.scard("active_consumers"):
#         redis_client.delete("consumer_count", "topics")

# def create_topics(topics):
#     admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
#     existing_topics = admin_client.list_topics()
#     new_topics = [
#         NewTopic(name=t, num_partitions=1, replication_factor=1)
#         for t in topics if t not in existing_topics
#     ]
#     if new_topics:
#         admin_client.create_topics(new_topics)
#     admin_client.close()

# def message_receiver(msg, logical_time, repcl, process_id):
#     msg = msg.value
#     logical_time = max(msg["logical_time"], logical_time) + 1
#     pt = time_to_seconds(msg["physical_time"])
#     repcl_m = RepCl.from_dict(msg["repcl"])
#     repcl = update_repcl_receive(repcl, repcl_m, pt, process_id, EPSILON, I)
#     msg["logical_time"] = logical_time
#     msg["repcl"] = repcl.to_dict()
#     print(f"Received:\n{msg}\n", flush=True)
#     return msg["process"], logical_time, repcl

# def message_sender(consumer_name, topic, message, logical_time, physical_time, producer, next_recipient, repcl, process_id):
#     logical_time += 1
#     pt = time_to_seconds(physical_time)
#     repcl = update_repcl_send(repcl, pt, process_id, EPSILON, I)
#     message.update({
#         "logical_time": logical_time,
#         "physical_time": physical_time,
#         "repcl": repcl.to_dict()
#     })
#     producer.send(topic, value=message)
#     producer.flush()
#     print(f"Sending to {next_recipient}:\n{message}\n", flush=True)
#     return logical_time, repcl

# def synchronize_clock(msg, physical_time):
#     sync_str = msg.value["sync_time"]
#     physical_time[:] = list(map(int, sync_str.split(':')))
#     print(f"Syncing {physical_time}", flush=True)
#     return physical_time

# def update_physical_time(physical_time):
#     drift = random.randint(0, 5)
#     physical_time[2] += drift
#     while physical_time[2] >= 60:
#         physical_time[2] -= 60
#         physical_time[1] += 1
#     while physical_time[1] >= 60:
#         physical_time[1] -= 60
#         physical_time[0] += 1
#     physical_time[0] %= 24
#     return physical_time

# def main():
#     consumer_name, topics = register_consumer()
#     process_id = int(consumer_name)
#     repcl = RepCl(mx=0, offsets={}, counters={})

#     create_topics(topics)

#     consumer = KafkaConsumer(
#         sync_topic, *topics,
#         bootstrap_servers=bootstrap_servers,
#         auto_offset_reset='earliest',
#         group_id=f'group-{consumer_name}-{generate_uuid()}',
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
#         auto_offset_reset='latest',
#         group_id=None,
#         enable_auto_commit=False,
#         value_deserializer=lambda v: json.loads(v.decode('utf-8'))
#     )

#     sync_consumer.poll(timeout_ms=1000)
#     for partition in sync_consumer.assignment():
#         sync_consumer.seek_to_end(partition)

#     print(f"Registered Consumer: {consumer_name} | Subscribed Topics: {consumer.subscription()}", flush=True)

#     logical_time = 0
#     physical_time = list(map(int, time.strftime("%H:%M:%S").split(':')))
#     message = {
#         "flag": "1",
#         "process": consumer_name,
#         "logical_time": logical_time,
#         "physical_time": physical_time,
#         "event": f"Hi! I am from {consumer_name}",
#         "repcl": repcl.to_dict()
#     }

#     def listen_to_sync():
#         while not stop_event.is_set():
#             try:
#                 messages = sync_consumer.poll(timeout_ms=1000)
#                 for _, msg_list in messages.items():
#                     for msg in msg_list:
#                         if msg.value["flag"] == "0":
#                             with time_lock:
#                                 synchronize_clock(msg, physical_time)
#                                 print(f"Registered Consumer: {consumer_name} | Subscribed Topics: {consumer.subscription()}", flush=True)
#                                 print(redis_client.smembers("active_consumers"))
#             except Exception as e:
#                 if not stop_event.is_set():
#                     print(f"Sync thread error: {e}", flush=True)

#     sync_thread = threading.Thread(target=listen_to_sync, daemon=True)
#     sync_thread.start()
#     update_listener_thread = threading.Thread(target=listen_for_updates, args=(consumer_name, consumer), daemon=True)
#     update_listener_thread.start()

#     def periodic_random_sender():
#         nonlocal logical_time, physical_time, repcl
#         while not stop_event.is_set():
#             time.sleep(random.randint(1, 5))
#             with time_lock:
#                 recipients = redis_client.smembers("active_consumers")
#                 if hasattr(recipients, '__await__'):
#                     recipients = []
#                 elif isinstance(recipients, set):
#                     recipients = list(recipients)
#                 possible_recipients = [p for p in recipients if p != consumer_name]
#                 if possible_recipients:
#                     next_recipient = random.choice(possible_recipients)
#                     topic = f"{consumer_name}{next_recipient}"
#                     logical_time, repcl = message_sender(
#                         consumer_name, topic, message.copy(), logical_time, physical_time, producer, next_recipient, repcl, process_id
#                     )

#     sender_thread = threading.Thread(target=periodic_random_sender, daemon=True)
#     sender_thread.start()

#     try:
#         for msg in consumer:
#             if msg.value["flag"] == "0":
#                 continue
#             with time_lock:
#                 sender, logical_time, repcl = message_receiver(msg, logical_time, repcl, process_id)
#             time.sleep(5)
#             recipients = redis_client.smembers("active_consumers")
#             if hasattr(recipients, '__await__'):
#                 recipients = []
#             elif isinstance(recipients, set):
#                 recipients = list(recipients)
#             possible_recipients = [p for p in recipients if p != consumer_name]
#             if possible_recipients:
#                 next_recipient = random.choice(possible_recipients)
#                 topic = f"{consumer_name}{next_recipient}"
#                 with time_lock:
#                     logical_time, repcl = message_sender(
#                         consumer_name, topic, message.copy(), logical_time, physical_time, producer, next_recipient, repcl, process_id
#                     )
#             consumer.commit()

#     except KeyboardInterrupt:
#         unregister_consumer(consumer_name)
#         stop_event.set()
#         sync_thread.join(timeout=2)
#         sync_consumer.close()
#         consumer.commit()
#         consumer.close()
#         producer.close()

# if __name__ == "__main__":
#     main()


















































from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import redis
import time
import json
import random
import threading
import uuid
from datetime import datetime

bootstrap_servers = 'kafka:9092'
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
sync_topic = 'synctopic'
update_topic = 'update_topic'
stop_event = threading.Event()
time_lock = threading.Lock()
# I = 100*0.000001  # Epoch size in seconds
# EPSILON = 1000*0.000001  # Maximum clock skew in epochs
I = 100000  # Epoch size in microseconds
EPSILON = 1000000   # Maximum clock skew in epochs
MAX_PROCESSES = 10  # Fixed list size for bitmap, offsets, counters

class RepCl:
    def __init__(self, mx, bitmap, offsets, counters):
        # Validate inputs
        if not (isinstance(bitmap, list) and isinstance(offsets, list) and isinstance(counters, list)):
            raise TypeError("bitmap, offsets, and counters must be lists")
        if not (len(bitmap) == len(offsets) == len(counters) == MAX_PROCESSES):
            raise ValueError(f"Lists must have length {MAX_PROCESSES}")
        self.mx = mx
        self.bitmap = bitmap  # List of 10 booleans (0 or 1)
        self.offsets = offsets  # List of 10 integers
        self.counters = counters  # List of 10 integers

    def to_dict(self):
        return {
            "mx": self.mx,
            "bitmap": self.bitmap,
            "offsets": self.offsets,
            "counters": self.counters
        }

    @classmethod
    def from_dict(cls, d):
        if not all(key in d for key in ["mx", "bitmap", "offsets", "counters"]):
            raise ValueError("Invalid RepCl dictionary")
        if not (isinstance(d["bitmap"], list) and isinstance(d["offsets"], list) and isinstance(d["counters"], list)):
            raise TypeError("Deserialized bitmap, offsets, and counters must be lists")
        if not (len(d["bitmap"]) == len(d["offsets"]) == len(d["counters"]) == MAX_PROCESSES):
            raise ValueError(f"Deserialized lists must have length {MAX_PROCESSES}")
        return cls(d["mx"], d["bitmap"], d["offsets"], d["counters"])

    def traverse_bitmap(self):
        """Algorithm 4.1: Return indices where bitmap is 1."""
        return [i for i, bit in enumerate(self.bitmap) if bit == 1]

    def get_offset_at_index(self, index):
        """Algorithm 4.3: Get offset at index."""
        if self.bitmap[index] == 1:
            return self.offsets[index]
        return EPSILON

    def set_offset_at_index(self, index, new_offset):
        """Algorithm 4.4: Set offset at index."""
        new_offsets = self.offsets.copy()
        new_bitmap = self.bitmap.copy()
        new_offsets[index] = new_offset
        new_bitmap[index] = 1 if new_offset < EPSILON else 0
        return new_bitmap, new_offsets

    def remove_offset_at_index(self, index):
        """Algorithm 4.5: Remove offset at index."""
        new_offsets = self.offsets.copy()
        new_bitmap = self.bitmap.copy()
        new_bitmap[index] = 0
        new_offsets[index] = EPSILON
        return new_bitmap, new_offsets

    def shift(self, newmx):
        """Algorithm 4.6: Shift operation."""
        delta = newmx - self.mx
        new_bitmap = [0] * MAX_PROCESSES
        new_offsets = [EPSILON] * MAX_PROCESSES
        for k in self.traverse_bitmap():
            new_offset = self.offsets[k] + delta
            if new_offset < EPSILON:
                new_bitmap[k] = 1
                new_offsets[k] = new_offset
        return RepCl(newmx, new_bitmap, new_offsets, self.counters)

    def merge_same_epoch(self, other):
        """Algorithm 4.7: Merge offsets of two timestamps with same mx."""
        if self.mx != other.mx:
            raise ValueError("Cannot merge timestamps with different mx")
        ts = RepCl(self.mx, [0] * MAX_PROCESSES, [EPSILON] * MAX_PROCESSES, [0] * MAX_PROCESSES)
        for k in range(MAX_PROCESSES):
            offset_self = self.get_offset_at_index(k)
            offset_other = other.get_offset_at_index(k)
            min_offset = min(offset_self, offset_other)
            if min_offset < EPSILON:
                ts.bitmap[k] = 1
                ts.offsets[k] = min_offset
        return ts

    def equal_offset(self, other):
        """Algorithm 4.8: Check if offsets are equal."""
        if self.mx != other.mx:
            return False
        for k in range(MAX_PROCESSES):
            if self.get_offset_at_index(k) != other.get_offset_at_index(k):
                return False
        return True

def update_repcl_send(repcl, pt, process_id):
    """Algorithm 4.9: Update RepCl for send/local event."""
    epoch = int(pt / I)
    newmx = max(repcl.mx, epoch)
    new_offset = newmx - epoch
    if repcl.mx == newmx and repcl.get_offset_at_index(process_id) == new_offset:
        new_counters = repcl.counters.copy()
        new_counters[process_id] += 1
        return RepCl(repcl.mx, repcl.bitmap, repcl.offsets, new_counters)
    else:
        shifted_repcl = repcl.shift(newmx)
        new_bitmap, new_offsets = shifted_repcl.bitmap, shifted_repcl.offsets
        new_counters = [0] * MAX_PROCESSES
        if new_offset < EPSILON:
            new_bitmap, new_offsets = shifted_repcl.set_offset_at_index(process_id, new_offset)
            new_counters[process_id] = 1
        else:
            new_bitmap, new_offsets = shifted_repcl.remove_offset_at_index(process_id)
        return RepCl(newmx, new_bitmap, new_offsets, new_counters)

def update_repcl_receive(repcl_j, repcl_m, pt, process_id):
    """Algorithm 4.10: Update RepCl for receive event."""
    epoch = int(pt / I)
    newmx = max(repcl_j.mx, repcl_m.mx, epoch)
    ts_a = repcl_j.shift(newmx)
    ts_b = repcl_m.shift(newmx)
    ts_c = ts_a.merge_same_epoch(ts_b)
    new_counters = [0] * MAX_PROCESSES
    if ts_a.equal_offset(ts_c) and ts_b.equal_offset(ts_c):
        for k in range(MAX_PROCESSES):
            counter_a = ts_a.counters[k]
            counter_b = ts_b.counters[k]
            new_counters[k] = max(counter_a, counter_b)
        new_counters[process_id] = new_counters[process_id] + 1
        ts_c.bitmap[process_id] = 1 if ts_c.get_offset_at_index(process_id) < EPSILON else 0
    elif ts_a.equal_offset(ts_c):
        new_counters = ts_a.counters.copy()
        new_counters[process_id] += 1
        ts_c.bitmap[process_id] = 1 if ts_c.get_offset_at_index(process_id) < EPSILON else 0
    elif ts_b.equal_offset(ts_c):
        new_counters = ts_b.counters.copy()
        new_counters[process_id] += 1
        ts_c.bitmap[process_id] = 1 if ts_c.get_offset_at_index(process_id) < EPSILON else 0
    else:
        new_counters[process_id] = 1
        ts_c.bitmap[process_id] = 1 if ts_c.get_offset_at_index(process_id) < EPSILON else 0
    return RepCl(ts_c.mx, ts_c.bitmap, ts_c.offsets, new_counters)

# def time_to_seconds(time_list):
#     H, M, S = time_list
#     return H * 3600 + M * 60 + S

def generate_uuid():
    return str(uuid.uuid4())

def register_consumer():
    consumer_id = redis_client.incr("consumer_count") - 1
    if consumer_id >= MAX_PROCESSES:
        raise ValueError(f"Consumer ID {consumer_id} exceeds maximum processes {MAX_PROCESSES}")
    consumer_name = str(consumer_id)
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
    redis_client.srem("active_consumers", consumer_name)
    redis_client.delete(f"subscriptions:{consumer_name}")
    for topic in redis_client.hkeys("topics"):
        if consumer_name in topic:
            redis_client.hdel("topics", topic)
    if not redis_client.scard("active_consumers"):
        redis_client.delete("consumer_count", "topics")

# Commented not in use rn
def create_topics(topics):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    existing_topics = admin_client.list_topics()
    new_topics = [
        NewTopic(name=t, num_partitions=1, replication_factor=1)
        for t in topics if t not in existing_topics
    ]
    if new_topics:
        admin_client.create_topics(new_topics)
    admin_client.close()

def message_receiver(msg, logical_time, repcl, process_id):
    msg = msg.value
    logical_time = max(msg["logical_time"], logical_time) + 1
    pt = msg["physical_time"]
    repcl_m = RepCl.from_dict(msg["repcl"])
    repcl = update_repcl_receive(repcl, repcl_m, pt, process_id)
    msg["logical_time"] = logical_time
    msg["repcl"] = repcl.to_dict()
    print(f"Received:\n{msg}\n", flush=True)
    return msg["process"], logical_time, repcl

def message_sender(consumer_name, topic, message, logical_time, physical_time, producer, next_recipient, repcl, process_id):
    logical_time += 1
    # pt = time_to_seconds(physical_time)
    repcl = update_repcl_send(repcl, physical_time, process_id)
    message.update({
        "logical_time": logical_time,
        "physical_time": physical_time,
        "repcl": repcl.to_dict()
    })
    producer.send(topic, value=message)
    producer.flush()
    print(f"Sending to {next_recipient}:\n{message}\n", flush=True)
    return logical_time, repcl

def synchronize_clock(msg, physical_time):
    physical_time = msg.value["sync_time"]
    # physical_time[:] = list(map(int, sync_str.split(':')))
    print(f"Syncing {physical_time}", flush=True)
    return physical_time

def update_physical_time(physical_time):
    # drift = random.randint(0, 5)
    # physical_time[2] += drift
    # while physical_time[2] >= 60:
    #     physical_time[2] -= 60
    #     physical_time[1] += 1
    # while physical_time[1] >= 60:
    #     physical_time[1] -= 60
    #     physical_time[0] += 1
    # physical_time[0] %= 24
    return datetime.now().second * 1_000_000 + datetime.now().microsecond

def main():
    consumer_name, topics = register_consumer()
    process_id = int(consumer_name)
    # Initialize offsets with EPSILON for all except current process
    initial_offsets = [EPSILON] * MAX_PROCESSES
    initial_offsets[process_id] = 0
    initial_bitmap = [0] * MAX_PROCESSES
    initial_bitmap[process_id] = 1
    repcl = RepCl(mx=0, bitmap=initial_bitmap, offsets=initial_offsets, counters=[0] * MAX_PROCESSES)

    # create_topics(topics)

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
    physical_time = datetime.now().second * 1_000_000 + datetime.now().microsecond
    message = {
        "flag": "1",
        "process": consumer_name,
        "logical_time": logical_time,
        "physical_time": physical_time,
        "event": f"Hi! I am from {consumer_name}",
        "repcl": repcl.to_dict()
    }

    def listen_to_sync():
        while not stop_event.is_set():
            try:
                messages = sync_consumer.poll(timeout_ms=1000)
                for _, msg_list in messages.items():
                    for msg in msg_list:
                        if msg.value["flag"] == "0":
                            with time_lock:
                                synchronize_clock(msg, physical_time)
                                print(f"Registered Consumer: {consumer_name} | Subscribed Topics: {consumer.subscription()}", flush=True)
                                print(redis_client.smembers("active_consumers"))
            except Exception as e:
                if not stop_event.is_set():
                    print(f"Sync thread error: {e}", flush=True)

    sync_thread = threading.Thread(target=listen_to_sync, daemon=True)
    sync_thread.start()
    update_listener_thread = threading.Thread(target=listen_for_updates, args=(consumer_name, consumer), daemon=True)
    update_listener_thread.start()

    def periodic_random_sender():
        nonlocal logical_time, physical_time, repcl
        while not stop_event.is_set():
            time.sleep(random.uniform(0.5*0.00001, 1*0.00001))
            with time_lock:
                recipients = redis_client.smembers("active_consumers")
                if hasattr(recipients, '__await__'):
                    recipients = []
                elif isinstance(recipients, set):
                    recipients = list(recipients)
                possible_recipients = [p for p in recipients if p != consumer_name]
                if possible_recipients:
                    next_recipient = random.choice(possible_recipients)
                    topic = f"{consumer_name}{next_recipient}"
                    physical_time=update_physical_time(physical_time)
                    logical_time, repcl = message_sender(
                        consumer_name, topic, message.copy(), logical_time, physical_time, producer, next_recipient, repcl, process_id
                    )

    sender_thread = threading.Thread(target=periodic_random_sender, daemon=True)
    sender_thread.start()

    try:
        for msg in consumer:
            if msg.value["flag"] == "0":
                continue
            with time_lock:
                sender, logical_time, repcl = message_receiver(msg, logical_time, repcl, process_id)
            

            # UNCOMMENT this when we want to see the offsets changing clearly
            
            # time.sleep(5)
            # recipients = redis_client.smembers("active_consumers")
            # if hasattr(recipients, '__await__'):
            #     recipients = []
            # elif isinstance(recipients, set):
            #     recipients = list(recipients)
            # possible_recipients = [p for p in recipients if p != consumer_name]
            # if possible_recipients:
            #     next_recipient = random.choice(possible_recipients)
            #     topic = f"{consumer_name}{next_recipient}"
            #     with time_lock:
            #         logical_time, repcl = message_sender(
            #             consumer_name, topic, message.copy(), logical_time, physical_time, producer, next_recipient, repcl, process_id
            #         )
            # consumer.commit()

    except KeyboardInterrupt:
        unregister_consumer(consumer_name)
        stop_event.set()
        sync_thread.join(timeout=2)
        sync_consumer.close()
        consumer.commit()
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main()