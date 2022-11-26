import json
import random
from confluent_kafka import Consumer

c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
}
)

c.subscribe(['demo_python'])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    print(f"Received message: {msg.value().decode('utf-8')}")

c.close()
