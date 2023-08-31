from confluent_kafka import Consumer
import json
import math

import app.prognose as prognose

# Kafka broker configuration
bootstrap_servers = 'broker:29092'
group_id = 'consumer-group-1'


def get_min_duration(messages):
    min_duration = math.inf
    for msg in messages:
        if ((msg["end_time_loading"] - msg["start_time_loading"]) < min_duration):
            min_duration = msg["end_time_loading"] - msg["start_time_loading"]
    return min_duration


def get_max_duration(messages):
    max_duration = -math.inf
    for msg in messages:
        if ((msg["end_time_loading"] - msg["start_time_loading"]) > max_duration):
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

    min_start = min([(msg["start_time_loading"]) for msg in messages])
    max_start = max([(msg["start_time_loading"]) for msg in messages])

    min_duration = get_min_duration(messages)
    max_duration = get_max_duration(messages)
    # print("Min duration: ", min_duration)
    # print("Max duration: ", max_duration)

    min_demand = min([(msg["kwh"]) for msg in messages])
    max_demand = max([(msg["kwh"]) for msg in messages])

    return prognose.TaskSimEvCharging(min_duration, max_duration, min_demand, max_demand, min_start, max_start, power)


def generate_prognose(topic):

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
        # We want 10 messages
        messages = []

        while len(messages) < 10:
            # Poll for messages
            response = consumer.poll(1.0)
            if response is None:
                continue
            messages.append(json.loads(response.value().decode('utf-8')))

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

        result = prognose.simulate_ev_forecast(
            df=df, cfg=task_instance)  # type: ignore

        return result.to_json()

    except KeyboardInterrupt:
        # User interrupted
        pass

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the consumer to release resources
        consumer.close()
