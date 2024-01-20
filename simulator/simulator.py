from kafka import KafkaProducer
import json
import random

types_map = {
    'one': 1.0,
    'two': 2.0,
    'five': 5.0,
    'ten': 10.0
}

producer = KafkaProducer(bootstrap_servers='localhost:19092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in range(100):
    id = str(i)
    item_type = random.choice(['one', 'two', 'five', 'ten'])
    expected_weight = types_map[item_type]
    weight = random.uniform(expected_weight - expected_weight * 0.055, expected_weight + expected_weight * 0.055)

    data = {'id': id, 'type': 'type-' + item_type, 'weight': weight}
    producer.send('produced-items', data)

producer.flush()