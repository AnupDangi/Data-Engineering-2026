from confluent_kafka import Producer
import json, time, uuid, random
from datetime import datetime
import logging

# Configure logging to see the output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "acks": "all"
})

## think this like a real time zomato order events streaming system 


""" 
This producer will generate random order events for a food delivery app like Zomato.
"""

## list of restaurants dummy example
restaurants = ["Dominos", "McDonalds", "KFC", "Subway"]

def producer_pipeline(restaurants):
    while True:
        event = {
        "event_id": str(uuid.uuid4()),
        "order_id": random.randint(10000, 99999),
        "user_id": random.randint(1, 100),
        "restaurant": random.choice(restaurants),
        "amount": random.randint(150, 800),
        "event_time": datetime.utcnow().isoformat()
    }

        producer.produce(
            topic="zomato_orders",
            key=str(event["user_id"]),
            value=json.dumps(event)
        )

        logging.info(f"Produced event to zomato_orders: {event}")
        producer.poll(0)
        time.sleep(1)

if __name__ == "__main__":
    producer_pipeline(restaurants)