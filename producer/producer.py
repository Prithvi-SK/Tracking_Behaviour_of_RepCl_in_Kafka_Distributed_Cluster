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

request_topic = 'requesttopic'
response_topic = 'responsetopic'
bootstrap_servers = 'kafka:9092'


producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
consumer = KafkaConsumer(response_topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest', group_id='test-group')

# Send message to consumer
message = time.strftime("%H:%M:%S",time.localtime())
producer.send(request_topic, message.encode('utf-8'))
producer.flush()
print(f'Sent: {message}')
time.sleep(5)

# producer.send(request_topic, b'Hello, Consumer! Please process this.')
# producer.flush()
# print("Message sent to request-topic.")

# Wait for response
for msg in consumer:
    print(f"Received: {msg.value.decode()}")
    break  # Exit after receiving one response
