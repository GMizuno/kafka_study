from confluent_kafka import Producer
import json
from sseclient import SSEClient as EventSource
from time import sleep

namespace_dict = {-2: 'Media',
                  -1: 'Special',
                  0: 'main namespace',
                  1: 'Talk',
                  2: 'User', 3: 'User Talk',
                  4: 'Wikipedia', 5: 'Wikipedia Talk',
                  6: 'File', 7: 'File Talk',
                  8: 'MediaWiki', 9: 'MediaWiki Talk',
                  10: 'Template', 11: 'Template Talk',
                  12: 'Help', 13: 'Help Talk',
                  14: 'Category', 15: 'Category Talk',
                  100: 'Portal', 101: 'Portal Talk',
                  108: 'Book', 109: 'Book Talk',
                  118: 'Draft', 119: 'Draft Talk',
                  446: 'Education Program', 447: 'Education Program Talk',
                  710: 'TimedText', 711: 'TimedText Talk',
                  828: 'Module', 829: 'Module Talk',
                  2300: 'Gadget', 2301: 'Gadget Talk',
                  2302: 'Gadget definition', 2303: 'Gadget definition Talk'}


def wikimediaproducer(producer: Producer, datasource: list, topic) -> None:
    for data in datasource:
        producer.poll(0)
        m = json.dumps(data)
        producer.produce(topic, m)


def build_event(event_source: dict, namespace_dict: dict) -> dict:
    try:
        event_source['namespace'] = namespace_dict[event_source['namespace']]
    except KeyError:
        event_source['namespace'] = 'unknown'

    user_types = {True: 'bot', False: 'human'}

    user_type = user_types[event_source['bot']]

    event = {"id": event_source['id'],
             "domain": event_source['meta']['domain'],
             "namespace": event_source['namespace'],
             "title": event_source['title'],
             "timestamp": event_source['meta']['dt'],
             "user_name": event_source['user'],
             "user_type": user_type,
             "old_length": event_source['length']['old'],
             "new_length": event_source['length']['new']}

    return event

def eventsource(producer: Producer) -> list:
    url = 'https://stream.wikimedia.org/v2/stream/recentchange'
    change = []
    for event in EventSource(url):
        if event.event == 'message':
            try:
                event_data = json.loads(event.data)
                if event_data.get('type') == 'edit':
                    event_to_send = build_event(event_data, namespace_dict)
                    print(event_to_send)
                    sleep(.5)
                    wikimediaproducer(producer, [event_to_send], 'wiki_demo')
            except:
                pass
    return change

p = Producer({'bootstrap.servers': 'localhost:9092'})


eventsource(p)
p.flush()

