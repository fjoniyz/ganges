from kafka import KafkaProducer
import csv
import json
import yaml

def send_dataset(bootstrap_servers, dataset):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    headers = None
    for instance in dataset:
        print(instance)
        if not headers:
            headers = instance
        else:
            instance_dict = {headers[i]: instance[i] for i in range(len(instance))}
            msg = json.dumps(instance_dict)
            print(str(msg))
            producer.send('streams-pipe-output', value=str(msg).encode('utf-8'), key="record".encode('utf-8'))
            producer.flush()

if __name__ == "__main__":
    with open("config.yaml", 'r') as f:
        config = yaml.safe_load(f)
        
    print(config)
    with open(config["dataset"]) as f:
        dataset = list(csv.reader(f, delimiter=","))
        send_dataset(config["bootstrap_servers"], dataset)