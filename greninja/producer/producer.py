from kafka import KafkaProducer
import time
import json

sync_topic = 'synctopic'
bootstrap_servers = 'kafka:9092'

try:
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:
        current_time = time.strftime("%H:%M:%S", time.localtime())
        message = {"flag": "0", "sync_time": current_time}
        
        producer.send(sync_topic, value=message)
        producer.flush()
        
        print(f"Syncing {message}", flush=True)
        time.sleep(10)  # Synchronization interval

except KeyboardInterrupt:
    producer.close()