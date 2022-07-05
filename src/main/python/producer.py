import json
import os


producer.send(topic="athletes", value=json.dumps(dict(zip(columns, data))).encode('utf-8'))

# producer.send(topic="athletes", value=b"a").get(timeout=10)
print("Message sent")

consumer = KafkaConsumer("athletes", group_id="test", bootstrap_servers='0.0.0.0:9092')
# consumer.subscribe(["athletes"])

# print(consumer.poll(timeout_ms=1000))
for m in consumer:
    print(m.value)
consumer.close()
print("Message received")