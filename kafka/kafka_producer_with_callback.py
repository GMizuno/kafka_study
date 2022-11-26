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
        print(f'Message delivered \n\ttopic = {topic}, \n\tnum partition= {partition} \n\toffset = {offset}')
        print(f'Message delivered with: {value}')


def send_data(datas: list):
    for data in datas:
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        # Asynchronously produce a message. The delivery report callback will
        # be triggered from the call to poll() above, or flush() below, when the
        # message has been successfully delivered or failed permanently.
        m = json.dumps(data)
        print(m)
        p.produce('demo_python', m, callback=delivery_report)

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
send_data(some_data_source)
