from confluent_kafka import Consumer, KafkaError
import json
import time
import prognose
from types import SimpleNamespace
# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
group_id = 'my-consumer-group'
topic = 'input-test6'

def create_TaskSimEvCharging(x, power) :
    #each max is just min value plus one hour

    # Define the input date and time string
    # input_start_time_loading = x.start_time_loading
    # dt = datetime.fromisoformat(input_start_time_loading)
    # minutes_start_time_loading = dt.hour * 60 + dt.minute
    #
    # input_end_time_loading = x.end_time_loading
    # dt = datetime.fromisoformat(input_end_time_loading)
    # minutes_end_time_loading = dt.hour * 60 + dt.minute

    min_start = x.start_time_loading
    max_start = int(min_start) + 60
    min_duration = x.end_time_loading - x.start_time_loading
    max_duration = min_duration + 60
    min_demand = int(x.kwh)
    max_demand = int(x.loading_potential)
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
        power = [1, 2, 3, 4]
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
