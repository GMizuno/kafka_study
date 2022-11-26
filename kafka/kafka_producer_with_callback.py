import time

from confluent_kafka import Producer
import json
import random

p = Producer({'bootstrap.servers': 'localhost:9092'})


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        value = msg.value()
        print(f'Message delivered \n\ttopic = {topic}, \n\tpartition= {partition} \n\toffset = {offset}')
        print(f'Message delivered with: {value}')
        print('-'*100)


some_data_source = [
        {
                'msg_1': random.randint(0, 19),
        },
        {
                'msg_2': random.randint(0, 19),
        },
        {
                'msg_3': random.randint(0, 19),
        },
        {
                'msg_4': random.randint(0, 19),
        },
        {
                'msg_5': random.randint(0, 19),
        },
        {
                'msg_6': random.randint(0, 19),
        },
        {
                'msg_7': random.randint(0, 19),
        },
        {
                'msg_8': random.randint(0, 19),
        },
]

for data in some_data_source:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    m = json.dumps(data)
    p.produce('demo_python', m, callback=delivery_report) # Use strick-partition

p.flush()


some_data_source = [
        {
                'msg_1': random.randint(0, 19),
        },
        {
                'msg_2': random.randint(0, 19),
        },
        {
                'msg_3': random.randint(0, 19),
        },
        {
                'msg_4': random.randint(0, 19),
        },
        {
                'msg_5': random.randint(0, 19),
        },
        {
                'msg_6': random.randint(0, 19),
        },
        {
                'msg_7': random.randint(0, 19),
        },
        {
                'msg_8': random.randint(0, 19),
        },
]

for data in some_data_source:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    m = json.dumps(data)
    time.sleep(2)
    p.produce('demo_python', m, callback=delivery_report) # Use round robin
p.flush()