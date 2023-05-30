from multiprocessing import Process
from typing import Iterable, List
from kafka import KafkaProducer
import csv
import json
import yaml
from time import sleep
import logging
import os


def send_dataset(bootstrap_servers: List[str], dataset: Iterable, topic: str, delay: int = 0):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    headers = dataset[0]  # CSV headers from the first row
    logging.info(f"Sending {len(dataset)} messages to topic '{topic}' on {bootstrap_servers} in 3 seconds")
    sleep(3)

    for instance in dataset[1:]:
        instance_dict = {headers[i]: instance[i] for i in range(len(instance))}
        msg = json.dumps(instance_dict)
        # logging.info(f"{topic} {str(msg)}")
        producer.send(topic, value=str(msg).encode(
            'utf-8'), key="record".encode('utf-8'))
        producer.flush()
        sleep(delay)


def read_and_send(config: dict, dataset: str, topic: str):
    with open(dataset) as f:
        dataset = list(csv.reader(f, delimiter=","))

    send_dataset(config["bootstrap_servers"], dataset, topic, config["delay"])


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    with open(os.path.dirname(os.path.realpath(__file__)) + "/config.yaml", 'r') as f:
        config = yaml.safe_load(f)
    logging.info("Config:", config)

    processes = [Process(target=read_and_send, args=(config, dataset, topic))
                for dataset, topic in config["dataset_topic"].items()]
    
    for process in processes:
        process.start()
    for process in processes:
        process.join()

    logging.info("DONE")