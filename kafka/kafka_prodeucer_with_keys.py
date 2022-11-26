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
        key = msg.key()
        print(
                f'Message delivered \n\ttopic = {topic}, \n\tpartition= {partition} \n\toffset = {offset} \n\tkey = {key}'
        )
        print(f'Message delivered with: {value}')
        print('-' * 100)


# Only for study purpose
def calc_partiition(key):
    if key == 'msg_1':
        return 0
    elif key in ('msg_2', 'msg_3', 'msg_4', 'msg_5'):
        return 1
    return 2


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
    p.poll(0)

    m = json.dumps(data)
    key = list(data.keys())[0]
    p.produce(
            'demo_python',
            value=m,
            key=key,
            partition=calc_partiition(key),
            callback=delivery_report
    )
    p.flush()
