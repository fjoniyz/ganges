"""
This script is a simple sample generator for Kafka.
The provided Kafka Datasource visualizes those samples in Grafana.
"""

from confluent_kafka import Producer
from time import sleep
import random
import json
import uuid
import numpy as np

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

counter = 1

while True:
    data = {
        "ae_session_id": f"{uuid.uuid1()}",
        "building_type": "commercial",
        "urbanisation_level": random.randint(1, 5),
        "number_loading_stations": random.randint(1, 10),
        "number_parking_spaces": random.randint(1, 90),
        "start_time_loading": "2023-06-23T09:00:00",
        "end_time_loading": "2023-06-23T12:00:00",
        "loading_time": random.randint(10, 250),
        "kwh": random.uniform(15, 60),
        "loading_potential": random.randint(40, 110),
    }
    producer.produce("streams-input", value=json.dumps(data))
    
    noise = np.random.laplace(0, 1.0, 1)
    data["kwh"] = data["kwh"] + noise[0]
    producer.produce("streams-output", value=json.dumps(data))
    
    print(f"Sample #{counter} produced!")
    counter += 1
    producer.flush(1)
    sleep(0.5)
