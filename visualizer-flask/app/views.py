from app import app
from flask import render_template, jsonify, request
import matplotlib.pyplot as plt
import numpy as np
import base64
import io

from kafka import KafkaConsumer
# from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json

# Todo: get bootstrap_server from the frontend?
bootstrap_server = 'broker:29092'
consumers = {}
consumer_marker = ""
conf = {
    'bootstrap.servers': bootstrap_server,
    'group.id': '1',
    'auto.offset.reset': 'earliest'  # Set offset reset policy
}
globals()['messages'] = np.array([])


@app.route('/', methods=['GET', 'POST'])
def index():
    consumer_marker = ""
    kadmin = AdminClient(conf)
    topics = kadmin.list_topics().topics
    topics.pop('__consumer_offsets')

    # Handle form submission
    if request.method == 'POST':
        topic = request.form.get('topic', type=str, default='streams-input')
        num_messages = request.form.get('num_messages', type=int, default=100)

        if topic not in topics:
            return render_template('index.html', consumer_marker=consumer_marker, error='Topic does not exist')

        try:
            # kafka-python based consumer
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[bootstrap_server],
                value_deserializer=lambda m: json.loads(m.decode('ascii')),
                auto_offset_reset='earliest'  # Adjust this as needed
            )

            # confluent-kafka based consumer
            # consumer = Consumer(conf)
            # consumer.subscribe([topic])

            consumers[topic] = consumer

            # TODO: extend this to support multiple consumers in parallel
            consumer_marker = {
                "topic": topic,
                "num_messages": num_messages,
            }

        except Exception as e:
            return render_template('index.html', consumer_marker=consumer_marker, error=e, topics=topics)

    return render_template('index.html', consumer_marker=consumer_marker, error='', topics=topics)


@app.route('/graph')
def generate_graph():
    try:
        num_messages = request.args.get('num_messages', type=int, default=100)
        topic = request.args.get('topic', type=str, default='test')
        messages = globals()['messages']

        consumer = consumers[topic]

        records = consumer.poll(timeout_ms=200)
        for topic_data, consumer_records in records.items():
            for consumer_record in consumer_records:
                globals()['messages'] = np.append(
                    messages, consumer_record.value['value'])

        fig, axs = plt.subplots(2)
        line, = axs[0].plot([], [])
        line.set_data(
            range(len(messages[-num_messages:])), messages[-num_messages:])

        # Set the first plot to contain the streamed values
        axs[0].set_xticklabels([])
        axs[0].set_xticks([])
        axs[0].set_ylabel('Message Value')
        axs[0].set_title(f'Streamed Values for Topic: {topic}')
        axs[0].relim()
        axs[0].autoscale_view()

        # Set the second plot to contain the distribution of values
        sns.histplot(messages, ax=axs[1], color='blue')
        axs[1].set_xlabel('Values')
        axs[1].set_ylabel('Frequency')
        axs[1].set_title('Value Distribution')

        # Save the plot to a PNG image in memory
        image = io.BytesIO()
        plt.savefig(image, format='png')
        image.seek(0)
        plt.close(fig)
        # Encode the image to base64 string
        graph_url = base64.b64encode(image.getvalue()).decode()

        # Return the encoded image as JSON response
        return jsonify({'graph_url': graph_url})
    except Exception as e:
        return jsonify({'error': e.__str__()})
