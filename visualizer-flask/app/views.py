from app import app
from flask import render_template, jsonify, request
import matplotlib.pyplot as plt
import numpy as np
import base64
import io

#from kafka import KafkaConsumer
from confluent_kafka import Consumer, KafkaError
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import json

# Todo: should we get this from docker-compose or from the frontend?
bootstrap_server = 'broker:9092'
consumers = {}

@app.route('/', methods=['GET', 'POST'])
def index():
    consumer_marker = ""
    if request.method == 'POST':
        topic = request.form.get('topic', type=str, default='streams-input')
        num_messages = request.form.get('num_messages', type=int, default=100)
        conf = {
            'bootstrap.servers': 'broker:9092',
            'group.id': '1',
            'auto.offset.reset': 'earliest'  # Set offset reset policy
        }           
        try:
            # kafka-python based consumer
            #consumer = KafkaConsumer(
            #    topic,
            #    bootstrap_servers=[bootstrap_server],
            #    value_deserializer=lambda m: json.loads(m.decode('ascii')),
            #    auto_offset_reset='earliest'  # Adjust this as needed
            #)

            # This will always fail currently, can't connect to broker. Unclear why.
            consumer = Consumer(conf)
            consumer.subscribe([topic])
            consumers[topic] = consumer
            
            # TODO: extend this to support multiple consumers
            consumer_marker = {
                "topic": topic,
                "num_messages": num_messages,
            }

        except Exception as e:
            #print("Error:", e)
            return render_template('index.html', consumer_marker=consumer_marker, error='Topic does not exist')
        
    return render_template('index.html', consumer_marker=consumer_marker, error='')


@app.route('/graph')
def generate_graph():
    try:
        num_messages = request.args.get('num_messages', type=int, default=100)
        topic = request.args.get('topic', type=str, default='test')

        consumer = consumers[topic]

        messages = np.array([])

        fig, axs = plt.subplots(2)
        line, = axs[0].plot([], [])

        axs[0].set_xticklabels([])
        axs[0].set_xticks([])
        axs[0].set_ylabel('Message Value')
        axs[0].set_title(f'Streamed Values for Topic: {topic}')

        sns.histplot(messages, ax=axs[1], color='blue')
        axs[1].set_xlabel('Values')
        axs[1].set_ylabel('Frequency')
        axs[1].set_title('Value Distribution')

        records = consumer.poll(timeout_ms=1000)
        for topic_data, consumer_records in records.items():
            for consumer_record in consumer_records:
                messages = np.append(messages, consumer_record.value['value'])
        line.set_data(range(len(messages[-num_messages:])), messages[-num_messages:])

        axs[1].cla()
        sns.histplot(messages, ax=axs[1], color='blue')
        axs[1].set_xlabel('Values')
        axs[1].set_ylabel('Frequency')
        axs[1].set_title('Value Distribution')

        # Save the plot to a PNG image in memory
        image = io.BytesIO()
        plt.savefig(image, format='png')
        image.seek(0)

        # Encode the image to base64 string
        graph_url = base64.b64encode(image.getvalue()).decode()

        # Return the encoded image as JSON response
        return jsonify({'graph_url': graph_url})
    except:
        return render_template('index.html', error='Error generating graph')
