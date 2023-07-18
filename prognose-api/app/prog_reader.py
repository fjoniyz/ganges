from tracemalloc import start
from confluent_kafka import Consumer
import json

from pandas import Timedelta
import app.prognose as prognose
from types import SimpleNamespace
from datetime import datetime, timedelta

# Kafka broker configuration
bootstrap_servers = 'broker:29092'
group_id = 'consumer-group-1'
topic = 'input1'


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
def generate_prognose():

    # Set the random seed to the current day of the year to get repeatable results
    prognose.random.seed(prognose.pd.Timestamp.utcnow().dayofyear)

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

    try:
        # We want 100 messages
        messages = consumer.consume(100, 1)
        messageDict = {}
        power = [1, 2, 3, 4]        

        if messages is None:
            return "No message received"

        print(f"Received {len(messages)} messages")

        # Parse the messages
        for i in range(len(messages)):

            config = json.loads(
                messages[i].value(), object_hook=lambda d: SimpleNamespace(**d))

            # Process the message
            print(f"Received message: {messages[i].value().decode('utf-8')}")

            
            task_instance = create_TaskSimEvCharging(config, power)

            messageDict.update({f"col{i}": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                               task_instance.max_demand, task_instance.min_duration, task_instance.max_duration]})

        df = prognose.DataFrame(data=messageDict)

        config = json.loads(
                messages[0].value(), object_hook=lambda d: SimpleNamespace(**d))
        task_instance = create_TaskSimEvCharging(config, power)

        print(f"Dataframe: {df}")
        print(f"Task instance: {task_instance}")
        print(f"max_demand: {task_instance.max_demand}")
        print(f"min_demand: {task_instance.min_demand}")
        print(f"max_duration: {task_instance.max_duration}")
        print(f"min_duration: {task_instance.min_duration}")
        print(f"max_start: {task_instance.max_start}")
        print(f"min_start: {task_instance.min_start}")
        
        result = prognose.simulate_ev_forecast(
            df=df, cfg=task_instance)  # type: ignore
        print(f"Result: {result}")

        return result.to_json()

    except KeyboardInterrupt:
        # User interrupted
        pass

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the consumer to release resources
        consumer.close()