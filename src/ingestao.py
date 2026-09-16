from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

endpoints = ['login', 'search', 'checkout']

while True:
    event = {'endpoint': random.choice(endpoints),
             'timestamp': int(time.time() * 1000),
             'count': 1}
    producer.send('events', event)
    time.sleep(1)
