# EmoStream
EmoStream: Concurrent Emoji Broadcast over Event-Driven Architecture Implementing a scalable system that enhances the viewer experience on platforms like Youtube by capturing and processing billions of user-generated emojis in real time.

The project uses a combination of Kafka, Spark and Flask
1. Kafka producer is used to send the emoji from client to aggregator where it is received by a kafka consumer
2. Aggregator file uses spark to scale down the emoji that it receives within a 2 second window and refresh the count after
3. From the spark aggregator a pub-sub architecture is set up to receive the scaled down message. There are two clusters that have their own cluster publishers. Each cluster has two subscribers.
4. Each subscriber can have a maximum of two clients connected to it. The subscribers send the aggregated message via a flask enpoint. Each subscriber is connected to a different port. The client has to connect to the respective port based on availability.

**To simulate the real world application we have each client posting 100 emojis a second**\
**Make sure to set up all other files before running the client file**
