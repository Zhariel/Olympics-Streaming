from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
import json
import os

# producer = KafkaProducer(bootstrap_servers='0.0.0.0:9092', api_version=(0, 10, 2))

# columns = ["link", "firstname", "name", "event", "year", "medal1", "medal2", "medal3", "medal4"]
# data = ["https://olympics.com/en/athletes/dongqi-chen","Dongqi" "CHEN","Tokyo 2020",1988.0,0,0,0,0]

# producer.send(topic="athletes", value=json.dumps(dict(zip(columns, data))).encode('utf-8'))
# print("sent")


consumer = KafkaConsumer("athletes", group_id="test", bootstrap_servers='0.0.0.0:9092')
consumer.subscribe(["athletes"])

print(consumer.poll(timeout_ms=1000))
for m in consumer:
    print(m.value)
consumer.close()
print("Message received")