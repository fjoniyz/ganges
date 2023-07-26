import math

from confluent_kafka import Consumer, KafkaError
import json
import prognose
from types import SimpleNamespace
# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
group_id = 'my-consumer-group'
topic = 'input-test6'

def create_TaskSimEvCharging(message, power):
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

    duration_message = message.endTimeLoading - message.startTimeLoading
    if min_start > message.startTimeLoading:
        min_start = int(message.startTimeLoading)
    if max_start < message.startTimeLoading:
        max_start = int(message.startTimeLoading) + 60
    if duration_message < min_duration:
        min_duration = duration_message
    if duration_message > max_duration:
        max_duration = duration_message + 20
    if min_demand > message.kwh:
        min_demand = int(message.kwh)
    if max_demand < message.kwh:
        max_demand = int(message.kwh) + 20

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

# Start consuming messages
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, continue polling
                continue
            else:
                # Error occurred
                print(f"Error: {msg.error()}")
                break
        x = json.loads(msg.value(), object_hook=lambda d: SimpleNamespace(**d))
        # Process the message
        print(f"Received message: {msg.value().decode('utf-8')}")
        prognose.random.seed(prognose.pd.Timestamp.utcnow().dayofyear)
        power = [11.0, 22.0]
        task_instance = create_TaskSimEvCharging(x, power)
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
