Create a kafka topic named `zomato_delivery_events` with 3 partitions and a replication factor of 1. You can use the following command:

```bash
kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic zomato_delivery_events \
  --partitions 3 \
  --replication-factor 1
```

Make sure your Kafka server is running locally on port 9092 before executing the above command.

Replaced the `create_kafka_producer` function in `kafka_producer.py` to use `confluent_kafka.Producer` instead of `KafkaProducer` from `kafka-python`. Updated the configuration parameters accordingly.

to run it

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic zomato_delivery_events \
  --from-beginning
```

This will allow you to see the messages being produced to the `zomato_delivery_events` topic in real-time.

and next go to `learntools/miniproject2/consumer/main.py` to implement the consumer logic.
