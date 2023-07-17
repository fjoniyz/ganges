from confluent_kafka import Consumer, KafkaError
import json
import prognose
from types import SimpleNamespace


# Kafka broker configuration
bootstrap_servers = 'broker:29092'
group_id = 'consumer-group-1'
topic = 'input-test'


def create_TaskSimEvCharging(config, power):
    # each max is just min value plus one hour

    # Define the input date and time string
    # input_start_time_loading = x.start_time_loading
    # dt = datetime.fromisoformat(input_start_time_loading)
    # minutes_start_time_loading = dt.hour * 60 + dt.minute

    # input_end_time_loading = x.end_time_loading
    # dt = datetime.fromisoformat(input_end_time_loading)
    # minutes_end_time_loading = dt.hour * 60 + dt.minute

    min_start = config.start_time_loading
    max_start = int(min_start) + 60
    min_duration = config.end_time_loading - config.start_time_loading
    max_duration = min_duration + 60
    min_demand = int(config.kwh)
    max_demand = int(config.loading_potential)

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
        task_instance = None

        if messages is None:
            return "No message received"
        if messages.error():
            if messages.error().code() == KafkaError._PARTITION_EOF:
                # End of partition, continue polling
                return "End of partition"
            else:
                # Error occurred
                print(f"Error: {messages.error()}")

        # Parse the messages
        for i in range(len(messages)):

            config = json.loads(
                messages[i].value(), object_hook=lambda d: SimpleNamespace(**d))

            # Process the message
            print(f"Received message: {messages[i].value().decode('utf-8')}")

            power = [1, 2, 3, 4]
            task_instance = create_TaskSimEvCharging(config, power)

            messageDict.update({f"col{i}": [task_instance.max_start, task_instance.min_start, task_instance.min_demand,
                               task_instance.max_demand, task_instance.min_duration, task_instance.max_duration]})

        df = prognose.DataFrame(data=messageDict)

        result = prognose.simulate_ev_forecast(df=df, cfg=task_instance)
        print(result)

        return result

    except KeyboardInterrupt:
        # User interrupted
        pass

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the consumer to release resources
        consumer.close()
