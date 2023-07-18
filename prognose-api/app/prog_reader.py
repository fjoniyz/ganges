from confluent_kafka import Consumer
import json
import calendar
import time

import app.prognose as prognose

# Kafka broker configuration
bootstrap_servers = 'broker:29092'
group_id = 'consumer-group-1'


def create_TaskSimEvCharging(x, power):
    min_start = int(min(x["start_time_loading"]) / 16581777)
    max_start = int(max(x["start_time_loading"]) / 16581777)

    min_duration = int(min(x["duration"]))
    max_duration = int(max(x["duration"]))

    min_demand = int(min(x["kwh"]))
    max_demand = int(max(x["kwh"]))

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
        # We want 60 messages
        messages = []

        while len(messages) < 60:
            # Poll for messages
            response = consumer.poll(1.0)
            if response is None:
                continue
            messages.append(response)

        for i in range(len(messages)):
            messages[i] = json.loads(messages[i].value().decode('utf-8'))

        power = [1, 2, 3, 4]
        data_set = {}
        data_set["start_time_loading"] = []
        data_set["end_time_loading"] = []
        data_set["kwh"] = []

        for message in messages:
            data_set["start_time_loading"] += [calendar.timegm(
                time.strptime(message["start_time_loading"], '%Y-%m-%d %H:%M:%S'))]
            data_set["end_time_loading"] += [calendar.timegm(
                time.strptime(message["end_time_loading"], '%Y-%m-%d %H:%M:%S'))]
            data_set["kwh"] += [int(message["kwh"])]

        data_set["duration"] = [
            x - y for x, y in zip(data_set["end_time_loading"], data_set["start_time_loading"])]

        task_instance = create_TaskSimEvCharging(data_set, power)

        """
        print(f"Task instance: {task_instance}")
        print(f"max_demand: {task_instance.max_demand}")
        print(f"min_demand: {task_instance.min_demand}")
        print(f"max_duration: {task_instance.max_duration}")
        print(f"min_duration: {task_instance.min_duration}")
        print(f"max_start: {task_instance.max_start}")
        print(f"min_start: {task_instance.min_start}")
        """

        d = {
            "col1": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
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
