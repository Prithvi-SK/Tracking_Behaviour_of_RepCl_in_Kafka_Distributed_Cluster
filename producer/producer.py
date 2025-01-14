from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='kafka:9092')
topic = 'clkclk'

# for i in range(10):
message = time.strftime("%H:%M:%S",time.localtime())
producer.send(topic, message.encode('utf-8'))
print(f'Sent: {message}')
time.sleep(5)

producer.close()
