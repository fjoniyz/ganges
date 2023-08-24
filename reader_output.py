import math
from confluent_kafka import Consumer, KafkaError
import json
import prognose
from types import SimpleNamespace
import redis
from numpy.linalg import norm
import numpy as np

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
group_id = 'my-consumer-group'
topic = 'output-test'
redis_port = 6379



def information_loss(message):
    '''
    returns the information loss (with euclidan distance) of anonymizes messages 
    compared to the original messages
    '''
    r = redis.Redis(host='localhost', port=6379)
    all_keys = r.keys('*')
    all_values = []
    information_loss = 0
    for key in all_keys:
        all_values.append({'id':key, 'values':r.hgetall(key)})

    for value in all_values:
        if message['id'] == value['values']['id'.encode('utf-8')].decode('utf-8'):
            print("Message and value are the same.")
            for a in value['values'].keys():
                if type(message[a.decode('utf-8')]) == float:
                    first = float(value['values'][a].decode('utf-8'))
                    second = message[a.decode('utf-8')]
                    # creating the euclidian distance
                    information_loss += norm(np.array([first])- np.array([second]))
    return information_loss

def get_min_duration(messages):
    min_duration = math.inf
    for msg in messages:
        if((msg["end_time_loading"] - msg["start_time_loading"]) < min_duration):
            min_duration = msg["end_time_loading"] - msg["start_time_loading"]
    return min_duration

def get_max_duration(messages):
    max_duration = -math.inf
    for msg in messages:
        if((msg["end_time_loading"] - msg["start_time_loading"]) > max_duration):
            max_duration = msg["end_time_loading"] - msg["start_time_loading"]
    return max_duration

def create_TaskSimEvCharging(messages, power):
    # each max is just min value plus one hour

    min_start = math.inf
    min_duration = math.inf
    min_demand = math.inf
    max_start = -math.inf
    max_demand = -math.inf
    max_duration = -math.inf

    #TODO: the + 60 and + 20 values are just there because we only do one message at a time for the moment
    #We're going to need something like:
    # min_start = int(min(x["start_time_loading"]))
    # max_start = int(max(x["start_time_loading"]))
    #
    # min_duration = int(min(x["duration"]))
    # max_duration = int(max(x["duration"]))
    #
    # min_demand = int(min(x["kwh"]))
    # max_demand = int(max(x["kwh"]))

    min_start = min([(msg["start_time_loading"]) for msg in messages])
    max_start = max([(msg["start_time_loading"]) for msg in messages])
    
    min_duration = get_min_duration(messages)
    max_duration = get_max_duration(messages)
    print("Min duration: ", min_duration)
    print("Max duration: ", max_duration)
    
    min_demand = min([(msg["kwh"]) for msg in messages])
    max_demand = max([(msg["kwh"]) for msg in messages])

    return prognose.TaskSimEvCharging(min_duration, max_duration, min_demand, max_demand, min_start, max_start, power)

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
}

# Create Kafka consumer
consumer = Consumer(consumer_config)

# Subscribe to the topic
consumer.subscribe([topic])

messages = []

# Start consuming messages
try:
    while True:
<<<<<<< HEAD
        while(len(messages) < 4):
            msg = consumer.poll(1.0)

            if msg is None:
=======
        msg = consumer.poll(1.0)
        if msg is None or msg.value().decode('utf-8') == "[]":
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, continue polling
>>>>>>> dd857acf5eb28d79f1477dae5baffddded0d6489
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
<<<<<<< HEAD
                messages.append(json.loads(msg.value().decode('utf-8')))
                
        # Process the message
=======
                # Error occurred
                print(f"Error: {msg.error()}")
                break
        x = json.loads(msg.value(), object_hook=lambda d: SimpleNamespace(**d))
        x_information_loss = information_loss(x[0].__dict__)
      
        # Process the message
        print(f"Received message: {msg.value().decode('utf-8')}")
        print("Information loss of message: ", x_information_loss)
>>>>>>> dd857acf5eb28d79f1477dae5baffddded0d6489
        prognose.random.seed(prognose.pd.Timestamp.utcnow().dayofyear)
        power = [11.0, 22.0]
        task_instance = create_TaskSimEvCharging(messages, power)
        messages = []
        d = {"col1": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                    task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
            "col2": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                    task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
            "col3": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                    task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
            "col4": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                    task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
            "col5": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                    task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
            "col7": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                    task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
            "col8": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                    task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
            "col9": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                    task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
            "col10": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                    task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
            "col11": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                    task_instance.max_demand, task_instance.min_duration, task_instance.max_duration],
            }
        df = prognose.DataFrame(data=d)
        print(prognose.simulate_ev_forecast(df=df, cfg=task_instance))

except KeyboardInterrupt:
    # User interrupted
    pass

finally:
    # Close the consumer to release resources
    consumer.close()
