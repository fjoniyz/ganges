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
topic = 'output1'
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
        byte_str = value['id']
        id_integer = int(byte_str)
        if message['id'] == id_integer:
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
        if((int(msg[0]["end_time_loading"]) - int(msg[0]["start_time_loading"])) < min_duration):
            min_duration = int(msg[0]["end_time_loading"]) - int(msg[0]["start_time_loading"])
    return min_duration

def get_max_duration(messages):
    max_duration = -math.inf
    for msg in messages:
        if((int(msg[0]["end_time_loading"]) - int(msg[0]["start_time_loading"])) > max_duration):
            max_duration = int(msg[0]["end_time_loading"]) - int(msg[0]["start_time_loading"])
    return max_duration

def create_TaskSimEvCharging(messages, power):
    # each max is just min value plus one hour

    min_start = math.inf
    min_duration = math.inf
    min_demand = math.inf
    max_start = -math.inf
    max_demand = -math.inf
    max_duration = -math.inf

    min_start = min([(msg[0]["start_time_loading"]) for msg in messages])
    max_start = max([(msg[0]["start_time_loading"]) for msg in messages])
    
    min_duration = get_min_duration(messages)
    max_duration = get_max_duration(messages)
    
    min_demand = min([(msg[0]["kwh"]) for msg in messages])
    max_demand = max([(msg[0]["kwh"]) for msg in messages])

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
        info_loss = 0
        while(len(messages) < 4):
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                message = json.loads(msg.value().decode('utf-8'))
                info_loss += information_loss(message[0])
                messages.append(message)
                print("messages: ", messages)
        print("The information Loss of this forecast is: ", info_loss)       
        # Process the message
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
