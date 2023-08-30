import math
from confluent_kafka import Consumer, KafkaError
import json
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import prognose
import redis
from numpy.linalg import norm
import numpy as np

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
group_id = 'my-consumer-group'
topic = 'output16'
redis_port = 6379

def information_loss(message):
    '''
    returns the information loss (with euclidan distance) of anonymizes messages 
    compared to the original messages
    '''
    r = redis.Redis(host='localhost', port=6379)
    all_keys = r.keys('*')
    all_values = []
    return_value = None
    information_loss = 0
    for key in all_keys:
        all_values.append({'id':key, 'values':r.hgetall(key)})

    for value in all_values:
        byte_str = value['id']
        id_integer = int(byte_str)
        if int(message['id']) == id_integer:
            return_value = {k.decode('utf8'): v.decode('utf8') for k, v in value['values'].items()}
            for a in value['values'].keys():
                if type(message[a.decode('utf-8')]) == float:
                    first = float(value['values'][a].decode('utf-8'))
                    second = message[a.decode('utf-8')]
                    # creating the euclidean distance
                    information_loss += norm(np.array([first])- np.array([second]))
    return information_loss, return_value

def get_min_duration(messages):
    min_duration = math.inf
    for msg in messages:
        if((int(msg["end_time_loading"]) - int(msg["start_time_loading"])) < min_duration):
            min_duration = int(msg["end_time_loading"]) - int(msg["start_time_loading"])
    return min_duration

def get_max_duration(messages):
    max_duration = -math.inf
    for msg in messages:
        if((int(msg["end_time_loading"]) - int(msg["start_time_loading"])) > max_duration):
            max_duration = int(msg["end_time_loading"]) - int(msg["start_time_loading"])
    return max_duration

def create_TaskSimEvCharging(messages, power):
    # each max is just min value plus one hour

    min_start = math.inf
    min_duration = math.inf
    min_demand = math.inf
    max_start = -math.inf
    max_demand = -math.inf
    max_duration = -math.inf

    min_start = min([int((msg["start_time_loading"])) for msg in messages])
    max_start = max([int((msg["start_time_loading"])) for msg in messages])
    
    min_duration = get_min_duration(messages)
    max_duration = get_max_duration(messages)
    
    min_demand = min([float((msg["kwh"])) for msg in messages])
    max_demand = max([float((msg["kwh"])) for msg in messages])

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
non_anon_messages = []
info_loss = []
diff_arr = []

# Start consuming messages
try:
    i = 0
    while i < 300:
        info_loss_value = 0
        while(len(messages) < 4):
            msg = consumer.poll(1.0)
            i += 1
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
                if message != []:
                    temp, return_value = information_loss(message[0])
                    info_loss_value += temp
                    messages.append(message[0])
                    non_anon_messages.append(return_value)
            print("The information Loss of this forecast is: ", info_loss_value)       
        # Process the message
        prognose.random.seed(prognose.pd.Timestamp.utcnow().dayofyear)
        power = [11.0, 22.0]
        task_instance = create_TaskSimEvCharging(messages, power)
        task_instance_from_value = create_TaskSimEvCharging(non_anon_messages, power)
        print("Task instance: ", task_instance.max_start)
        print("Task instance from values ", task_instance_from_value.max_start)
        messages = []
        non_anon_messages = []
        d_values = {"col1": [task_instance_from_value.max_start, task_instance_from_value.min_start, task_instance_from_value.min_demand,
                    task_instance_from_value.max_demand, task_instance_from_value.min_duration, task_instance_from_value.max_duration],
            "col2": [task_instance_from_value.max_start, task_instance_from_value.min_start, task_instance_from_value.min_demand,
                    task_instance_from_value.max_demand, task_instance_from_value.min_duration, task_instance_from_value.max_duration],
            "col3": [task_instance_from_value.max_start, task_instance_from_value.min_start, task_instance_from_value.min_demand,
                    task_instance_from_value.max_demand, task_instance_from_value.min_duration, task_instance_from_value.max_duration],
            "col4": [task_instance_from_value.max_start, task_instance_from_value.min_start, task_instance_from_value.min_demand,
                    task_instance_from_value.max_demand, task_instance_from_value.min_duration, task_instance_from_value.max_duration],
            "col5": [task_instance_from_value.max_start, task_instance_from_value.min_start, task_instance_from_value.min_demand,
                    task_instance_from_value.max_demand, task_instance_from_value.min_duration, task_instance_from_value.max_duration],
            "col7": [task_instance_from_value.max_start, task_instance_from_value.min_start, task_instance_from_value.min_demand,
                    task_instance_from_value.max_demand, task_instance_from_value.min_duration, task_instance_from_value.max_duration],
            "col8": [task_instance_from_value.max_start, task_instance_from_value.min_start, task_instance_from_value.min_demand,
                    task_instance_from_value.max_demand, task_instance_from_value.min_duration, task_instance_from_value.max_duration],
            "col9": [task_instance_from_value.max_start, task_instance_from_value.min_start, task_instance_from_value.min_demand,
                    task_instance_from_value.max_demand, task_instance_from_value.min_duration, task_instance_from_value.max_duration],
            "col10": [task_instance_from_value.max_start, task_instance_from_value.min_start, task_instance_from_value.min_demand,
                    task_instance_from_value.max_demand, task_instance_from_value.min_duration, task_instance_from_value.max_duration],
            "col11": [task_instance_from_value.max_start, task_instance_from_value.min_start, task_instance_from_value.min_demand,
                    task_instance_from_value.max_demand, task_instance_from_value.min_duration, task_instance_from_value.max_duration],
            } 
        d_msg = {"col1": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
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
        df_msg = prognose.DataFrame(data=d_msg)
        df_vals = prognose.DataFrame(data=d_values)
        info_loss.append(info_loss_value)
        diff_arr.append(abs(prognose.simulate_ev_forecast(df=df_msg, cfg=task_instance)['demand'].iloc[0] - prognose.simulate_ev_forecast(df=df_vals, cfg=task_instance_from_value)['demand'].iloc[0]))
        print("Diff arr ", diff_arr)
        print("Info loss ", info_loss)
    data_to_plot = []
    f = open("placeholder.txt", "r")
    for value in f:
        data_to_plot.append(float(value[:-2]))
    f.close()
    print(len(data_to_plot))
    df = pd.DataFrame({'20': data_to_plot, '200': info_loss})
    print("Df: ", df)
    chart = sns.violinplot(data=df, cut=0)
    chart.set_xlabel("eps")
    chart.set_ylabel("information loss")
    plt.show()

except KeyboardInterrupt:
    # User interrupted
    pass

finally:
    # Close the consumer to release resources
    consumer.close()
