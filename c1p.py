from kafka import KafkaConsumer, KafkaProducer
import json

# Initialize Kafka producer to send data to Cluster 1 subscriber topics
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Initialize Kafka consumer to consume messages from the main cluster's forward topic
consumer = KafkaConsumer(
    'aggregated_emoji_topic_cluster1', 
    bootstrap_servers='localhost:9092', 
    group_id='cluster1_group'
)

# Function to forward messages to 3 subscriber topics
def forward_to_subscribers(message):
    value = message.value.decode('utf-8')
    # Forward to all 3 subscriber topics
    producer.send('cluster1_subscriber1_topic', value=value.encode('utf-8'))
    producer.send('cluster1_subscriber2_topic', value=value.encode('utf-8'))
    

# Consume messages and forward to subscribers
for message in consumer:
    forward_to_subscribers(message)
    print(message)
