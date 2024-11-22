import random
import json
from datetime import datetime
from kafka import KafkaProducer
import threading
import time
import requests
import os
import csv
import signal

# Define Kafka topic for emoji input
PRODUCER_TOPIC = "emoji_topic"
CSV_FILE = "subscribers.csv"
SUBSCRIBERS = ["http://localhost:5001/receive_message", "http://localhost:5002/receive_message", "http://localhost:5003/receive_message", "http://localhost:5004/receive_message"]
MAX_USERS_PER_SUBSCRIBER = 2
# Emojis to randomly choose from
EMOJIS = ['\U0001f604', '\U0001f61e', '\u26bd', '\U0001f389', '\U0001f44d', '\U0001f44e']

# Emoji menu mapping numbers to emojis
EMOJI_MENU = {
    1: '\U0001f604',  # Happy
    2: '\U0001f61e',  # Sad
    3: '\u26bd',  # Sports
    4: '\U0001f389',  # Party
    5: '\U0001f44d',  # Thumbs Up
    6: '\U0001f44e'   # Thumbs Down
}

# Shared flag to control the random emoji generation
keep_generating = True
keep_running = True

# CSV Management
def initialize_csv():
    """Ensure the CSV file exists and is properly initialized."""
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["user_id", "subscriber"])  # Header row

def get_subscriber_counts():
    """Get the current user count for each subscriber."""
    counts = {subscriber: 0 for subscriber in SUBSCRIBERS}
    if not os.path.exists(CSV_FILE):
        return counts

    with open(CSV_FILE, mode="r", newline="") as file:
        reader = csv.reader(file)
        #next(reader, None)  # Skip header
        for _, subscriber in reader:
            if subscriber in counts:
                counts[subscriber] += 1
    return counts

def assign_subscriber(user_id):
    """Assign a subscriber to the user."""
    counts = get_subscriber_counts()
    for subscriber, count in counts.items():
        if count < MAX_USERS_PER_SUBSCRIBER:
            with open(CSV_FILE, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([user_id, subscriber])
            return subscriber
    return None  # No available subscribers

def remove_user_from_csv(user_id):
    """Remove the user from the CSV file."""
    if not os.path.exists(CSV_FILE):
        return

    with open(CSV_FILE, mode="r", newline="") as file:
        rows = list(csv.reader(file))

    with open(CSV_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        for row in rows:
            if row[0] != user_id:
                writer.writerow(row)

def wait_for_slot(user_id):
    """Wait for a slot to become free and assign the user."""
    print(f"User {user_id}: Waiting for a free subscriber slot...")
    while keep_running:
        subscriber = assign_subscriber(user_id)
        if subscriber:
            print(f"User {user_id}: Assigned to {subscriber}. Restarting polling...")
            # Restart polling for the correct subscriber
            flask_polling_thread = threading.Thread(target=receive_processed_messages_from_flask, args=(subscriber,))
            flask_polling_thread.daemon = True
            flask_polling_thread.start()
            break
        # Wait and check again
        time.sleep(1)


def handle_exit(signal_received, frame, user_id=None):
    """Handle termination to clean up resources."""
    global keep_running
    keep_running = False
    if user_id:
        print(f"User {user_id}: Disconnecting and freeing slot...")
        remove_user_from_csv(user_id)
    exit(0)

def generate_random_emojis(producer):
    """
    Generate 200 random emojis per second and send to Kafka.
    """
    global keep_generating
    print("Random emoji generation started...")
    while keep_generating:
        for _ in range(100):  # Simulate 200 emojis per second
            emoji = random.choice(EMOJIS)
            timestamp = datetime.now().isoformat()

            # Construct the message
            message = {
                "emoji": emoji,
                "timestamp": timestamp,
                "source": "random"
            }

            # Send message to Kafka
            producer.send(PRODUCER_TOPIC, value=message)

        # Wait for 1 second before generating the next batch
        time.sleep(1)

def input_emojis(producer, user_id,url):
    """
    Accept emoji input from the client via the terminal.
    """
    global keep_generating
    consumer_thread = threading.Thread(target=receive_processed_messages_from_flask, args=(url,))
    consumer_thread.daemon = True
    consumer_thread.start()
    print("Manual emoji input started...")
    print("Emoji Menu:")
    for num, emoji in EMOJI_MENU.items():
        print(f"{num}: {emoji}")

    while True:
        try:
            user_input = int(input("Enter the number corresponding to the emoji (or 0 to exit): "))
            if user_input == 0:
                print("Exiting manual input mode, stopping random generation, and unregistering.")
                keep_generating = False  # Stop random emoji generation
                remove_user_from_csv(user_id)  # Unregister the user
                print(f"User {user_id}: Unregistered successfully.")
                os._exit(0)  # Exit the program

            elif user_input in EMOJI_MENU:
                emoji = EMOJI_MENU[user_input]
                timestamp = datetime.now().isoformat()

                # Construct the message
                message = {
                    "emoji": emoji,
                    "timestamp": timestamp,
                    "source": "manual"
                }

                # Send message to Kafka
                producer.send(PRODUCER_TOPIC, value=message)
                print(f"Sent emoji '{emoji}' to Kafka.")
            else:
                print("Invalid input. Please choose a valid number.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def receive_processed_messages_from_flask(url):
    """
    Poll the Flask endpoint to receive and print processed messages.
    """
    flask_endpoint = url  # Flask endpoint URL
    with open(CSV_FILE, mode="r", newline="") as file:
        reader = csv.reader(file)
        CONSUMER_TOPIC = None  # Initialize the topic variable
        
        for row in reader:
            if len(row) >= 2 and row[0] == user_id:  # Ensure the row has at least 2 values
                CONSUMER_TOPIC = row[1]  # Assign the consumer topic
                break  # Exit the loop once the topic is found

    # Check if the topic was found
    if not CONSUMER_TOPIC:
        print(f"Error: No topic found for user_id '{user_id}'.")
        return


    print(f"Polling processed messages from Flask endpoint '{flask_endpoint}'...")
    while True:
        try:
            # Send a GET request to the Flask endpoint to retrieve processed messages
            response = requests.get(flask_endpoint)

            # Check if the response is successful
            if response.status_code == 200:
                message = response.json()  # Parse the JSON response
                for item in message:
                        emoji = item.get('emoji')
                        transformed_count = item.get('transformed_count', 0)

                        # Ensure emoji and transformed_count are valid before printing
                        if emoji and isinstance(transformed_count, int):
                            for _ in range(transformed_count):
                                print(emoji, end=" ")
            else:
                print(f"Error: Received status code {response.status_code} from Flask endpoint.")
        except requests.exceptions.RequestException as e:
            print(f"Error connecting to Flask endpoint: {e}")

        # Wait before sending another request
        time.sleep(1)


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=500
    )
    user_id = None
    while not user_id:
        entered_id = input("Enter your 3-digit user ID to start: ").strip()
        if len(entered_id) == 3 and entered_id.isdigit():
            user_id = entered_id
            print(f"User {user_id}: Registered successfully!")
        else:
            print("Invalid user ID. Please enter a 3-digit number.")

    # Try to assign a subscriber
    assigned_subscriber = assign_subscriber(user_id)
    if assigned_subscriber:
        print(f"User {user_id}: Assigned to {assigned_subscriber}.")
    else:
        wait_thread = threading.Thread(target=wait_for_slot, args=(user_id,))
        wait_thread.daemon = True
        wait_thread.start()

    # Start the random emoji generation in a separate thread
    random_thread = threading.Thread(target=generate_random_emojis, args=(producer,))
    random_thread.daemon = True
    random_thread.start()

    # Start the Flask message polling in a separate thread
    flask_polling_thread = threading.Thread(target=receive_processed_messages_from_flask, args=(assigned_subscriber,))
    flask_polling_thread.daemon = True
    flask_polling_thread.start()

    # Run the manual emoji input function in the main thread
    input_emojis(producer, user_id,assigned_subscriber)
