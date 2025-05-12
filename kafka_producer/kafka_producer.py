from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open("data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

for record in data:
    print(f"Sending: {record}")
    producer.send("real-estate-topic", record)
    time.sleep(1)
