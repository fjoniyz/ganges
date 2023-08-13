from argparse import ArgumentError
import datetime
from multiprocessing import Process
from typing import Iterable, List
from kafka import KafkaProducer
import csv
import json
import yaml
from time import sleep
import logging
import os
from generators_list import GENERATORS_LIST
import time

def preprocess_value(value):
    if type(value) == datetime.datetime:
        return str(value)
    return value

def send_dataset(bootstrap_servers: List[str], dataset: Iterable, topic: str, headers=[], delay: int = 0):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    if not headers:
        headers = dataset[0]  # CSV headers from the first row
        dataset = dataset[1:]
    logging.info(f"Sending messages to topic '{topic}' on {bootstrap_servers} in 3 seconds")
    sleep(3)

    for instance in dataset:
        instance_dict = {headers[i]: preprocess_value(instance[i]) for i in range(len(instance))}
        instance_dict['producer_timestamp'] = str(int(time.time() * 1000))
        msg = json.dumps(instance_dict)
        logging.info(f"{topic} {instance_dict['id']} {str(msg)}")
        producer.send(topic, value=str(msg).encode(
            'utf-8'), key=instance_dict["id"].encode('utf-8')) # change the key here??
        producer.flush()
        sleep(delay)


def read_and_send(config: dict, dataset: str, topic: str):
    """Send messages from CSV file"""
    if not dataset.endswith(".csv"):
        raise ArgumentError("In non-streamable mode, the dataset must be a .csv file")
    
    with open(dataset) as f:
        dataset = list(csv.reader(f, delimiter=","))
        for row in dataset:
            for i in range(len(row)):
                if row[i].replace(".", "").replace(",", "").isnumeric():
                    row[i] = float(row[i])

    send_dataset(bootstrap_servers=config["bootstrap_servers"], 
                 dataset=dataset, 
                 topic=topic,
                 delay=config["delay"])
    
def send_streaming(config: dict, dataset: str, topic: str):
    """Send messages generated on-the-fly"""
    send_dataset(bootstrap_servers=config["bootstrap_servers"], 
                 dataset=GENERATORS_LIST[dataset][1](),
                 headers=GENERATORS_LIST[dataset][0](), 
                 topic=topic, 
                 delay=config["delay"])

if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    with open(os.path.dirname(os.path.realpath(__file__)) + "/config.yaml", 'r') as f:
        config = yaml.safe_load(f)
    logging.info("Config:", config)
    
    if config["streaming"]:
        processes = [Process(target=send_streaming, args=(config, dataset, topic))
                    for dataset, topic in config["dataset_topic"].items()]
    else:
        processes = [Process(target=read_and_send, args=(config, dataset, topic))
                    for dataset, topic in config["dataset_topic"].items()]
    
    for process in processes:
        process.start()
    for process in processes:
        process.join()

    logging.info("DONE")
