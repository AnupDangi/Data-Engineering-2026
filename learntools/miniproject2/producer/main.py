from producer.csv_reader import stream_csv
from producer.rate_controller import rate_limit
from producer.event_builder import build_event
from producer.kafka_producer import create_kafka_producer
import json

CSV_PATH = "./data/ZomatoDataset.csv"
BATCH_SIZE = 100
INTERVAL_SECONDS = 1

KAFKA_BOOTSTRAP="localhost:9092"
KAFKA_TOPIC="zomato_delivery_events"

def main():

    ## create the producer 
    producer=create_kafka_producer(KAFKA_BOOTSTRAP)

    csv_stream = stream_csv(CSV_PATH)
    controlled_stream = rate_limit(
        csv_stream,
        batch_size=BATCH_SIZE,
        interval_seconds=INTERVAL_SECONDS
    )

    total_events = 0

    for batch in controlled_stream:
        for row in batch:
            event = build_event(row)

            # confluent_kafka uses produce() instead of send()
            producer.produce(
                topic=KAFKA_TOPIC,
                key=event["event_id"].encode('utf-8'),
                value=json.dumps(event).encode('utf-8')
            )

        producer.flush()
        total_events += len(batch)

        print(f"Published {len(batch)} events | Total: {total_events}")

    producer.close() ## but in real time system this will never be reached or we dont close the producer
    print("Kafka streaming completed.")



if __name__ == "__main__":
    main()
