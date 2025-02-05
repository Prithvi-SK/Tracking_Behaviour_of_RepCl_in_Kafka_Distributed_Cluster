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

request_topic = 'requesttopic'
response_topic = 'responsetopic'
bootstrap_servers = 'kafka:9092'


producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
consumer = KafkaConsumer(response_topic, 
                         bootstrap_servers=bootstrap_servers, 
                         auto_offset_reset='earliest', 
                         group_id='test-group', 
                         enable_auto_commit=True,
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

# Send message to consumer
message = time.strftime("%H:%M:%S",time.localtime())
message={"hello":"world"}
producer.send(request_topic, value=message)
producer.flush()
print(f'Sent: {message}')
time.sleep(5)

# producer.send(request_topic, b'Hello, Consumer! Please process this.')
# producer.flush()
# print("Message sent to request-topic.")

# Wait for response
for msg in consumer:
    print(f"raw {msg.value}")
    # try:
    #     data = json.loads(msg.value.decode('utf-8'))
    #     print(f"Received: {data}")
    # except json.JSONDecodeError as e:
    #     print(f"Skipping corrupted message: {e}")
    # print(f"Received: {msg.value}")
    break  # Exit after receiving one response
