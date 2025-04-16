# from kafka import KafkaProducer
# import time
# import json

# sync_topic = 'synctopic'
# bootstrap_servers = 'kafka:9092'

# try:
#     producer = KafkaProducer(
#         bootstrap_servers=bootstrap_servers,
#         value_serializer=lambda v: json.dumps(v).encode('utf-8')
#     )

#     while True:
#         current_time = time.strftime("%H:%M:%S", time.localtime())
#         message = {"flag": "0", "sync_time": current_time}
        
#         producer.send(sync_topic, value=message)
#         producer.flush()
        
#         print(f"Syncing {message}", flush=True)
#         time.sleep(10)  # Synchronization interval

# except KeyboardInterrupt:
#     producer.close()


from kafka import KafkaProducer
import time
import json
import redis
import os

producer_id = os.getenv("PRODUCER_ID", "default")
sync_topic = 'synctopic'
bootstrap_servers = 'kafka:9092'
redis_host = 'redis'
redis_port = 6379

r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

def attempt_leadership():
    # Try to acquire leadership by setting the key with expiration
    return r.set('producer:leader', producer_id, ex=15, nx=True)

def refresh_leadership():
    r.expire('producer:leader', 15)

def get_current_leader():
    return r.get('producer:leader')

try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        current_leader = get_current_leader()

        if current_leader == producer_id or attempt_leadership():
            # This instance is the leader
            current_time = time.strftime("%H:%M:%S", time.localtime())
            message = {"flag": "0", "sync_time": current_time, "by": producer_id}

            kafka_producer.send(sync_topic, value=message)
            kafka_producer.flush()

            print(f"[{producer_id}] Syncing {message}", flush=True)

            # Refresh leadership TTL to signal liveness
            refresh_leadership()
            time.sleep(10)

        else:
            print(f"[{producer_id}] Not leader. Current leader: {current_leader}", flush=True)
            time.sleep(5)  # Check again after short wait

except KeyboardInterrupt:
    kafka_producer.close()
