from kafka import KafkaConsumer
import json
import random

types_map = {
    'type-one': 1.0,
    'type-two': 2.0,
    'type-five': 5.0,
    'type-ten': 10.0
}

consumer = KafkaConsumer(bootstrap_servers='localhost:19092', value_deserializer=json.loads)
consumer.subscribe(['defective-items'])
for message in consumer:
    # print(message)
    id = message.value['id']
    item_type = message.value['type']
    weight = message.value['weight']
    expected_weight = types_map[item_type]

    deviation = abs(expected_weight - weight) / expected_weight * 100

    print("Item {} is defective. Expected weight = {}, measured weight = {}, deviation = {}%".format(id, expected_weight, weight, deviation))