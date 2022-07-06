from kafka import KafkaConsumer
from time import sleep


consumer = KafkaConsumer("processed", group_id="test", bootstrap_servers='0.0.0.0:9092', auto_offset_reset='latest')
consumer.subscribe(["processed"])

while True:
    print(consumer.poll(timeout_ms=1000))
    print("Message received")
    sleep(5)