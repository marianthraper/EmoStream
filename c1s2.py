import json
from kafka import KafkaConsumer
from flask import Flask, request, jsonify
import requests

# Flask app initialization
app = Flask(__name__)

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    'cluster1_subscriber2_topic',
    bootstrap_servers='localhost:9092',
    group_id='subscriber2_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Automatically deserialize JSON messages
)
processed_messages = []
# HTTP endpoint to receive the filtered message
@app.route('/receive_message', methods=['POST'])
def receive_message():
    # This endpoint will handle messages sent from the Kafka consumer
    data = request.json  # Parse the incoming JSON data
    print(f"Received message at endpoint: {data}")  # Log the received data
    
    # Store the received message in the processed_messages list
    processed_messages.append(data)
    
    return jsonify({"status": "Message received"}), 200



@app.route('/receive_message', methods=['GET'])
def get_processed_messages():
    """
    Retrieve and return the processed messages.
    """
    return jsonify(processed_messages), 200
    
# Start the Flask app in a background thread
def run_flask_app():
    app.run(port=5002)  # Set port number here


# Process messages from Kafka and send them to the HTTP endpoint
def process_kafka_messages():
    endpoint_url = 'http://localhost:5002/receive_message'  # URL of the HTTP endpoint

    for message in consumer:
        # Decode the message received from Kafka and post it directly
        raw_message = message.value

        # Extract only the required keys (filtering the data here)
        filtered_message = {
            "emoji": raw_message.get("emoji"),
            "transformed_count": raw_message.get("transformed_count")
        }

        # Print the filtered message to the terminal (logging)
        print(f"Subscriber 2 received and posting message: {filtered_message}")

        # Send the filtered message to the HTTP endpoint
        try:
            response = requests.post(endpoint_url, json=filtered_message)
            print(f"Message posted to endpoint, response status: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Error posting message to endpoint: {e}")


if __name__ == '__main__':
    import threading

    # Run Flask app in a separate thread
    flask_thread = threading.Thread(target=run_flask_app)
    flask_thread.daemon = True  # Ensure it exits when the main program does
    flask_thread.start()

    # Start processing Kafka messages
    process_kafka_messages()

