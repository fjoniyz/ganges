"""
This script is a simple sample generator for Kafka.
The provided Kafka Datasource visualizes those samples in Grafana.
"""

from confluent_kafka import Producer
from time import sleep
from random import random
import json
import uuid

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

counter = 1

while True:
    data = {"ae_session_id": f"{uuid.uuid1()}", "building_type":"commercial", "urbanisation_level": 0.8, "number_loading_stations": 5, "number_parking_spaces": 50, "start_time_loading": "2023-06-23T09:00:00", "end_time_loading": "2023-06-23T12:00:00", "loading_time": 180, "kwh": 20.5, "loading_potential": 80}
    producer.produce("streams-input", value=json.dumps(data))
    print(f"Sample #{counter} produced!")
    counter += 1
    producer.flush(1)
    sleep(1)
