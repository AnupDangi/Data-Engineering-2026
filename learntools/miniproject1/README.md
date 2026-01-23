# Mini Project

This mini project contains two main components:

## Kafka

### File: `producer_order.py`

This script is responsible for producing messages to a Kafka topic. It simulates or processes order data and sends it to the Kafka broker for further processing.

### Key Features:

- Produces order data to Kafka topics.
- Can be configured to work with different Kafka brokers and topics.

## Spark

### File: `spark_streaming_orders.py`

This script is responsible for consuming messages from a Kafka topic using Apache Spark Streaming. It processes the order data in real-time and performs necessary transformations or aggregations.

### Key Features:

- Consumes order data from Kafka topics.
- Processes data in real-time using Spark Streaming.
- Can be extended to perform analytics or save processed data to storage systems.

## How to Run

1. **Kafka Producer**:
   - Navigate to the `kafka` directory.
   - Run the `producer_order.py` script to start producing messages.

2. **Spark Streaming**:
   - Navigate to the `spark` directory.
   - Run the `spark_streaming_orders.py` script to start consuming and processing messages.

## Prerequisites

- Python 3.8+
- Kafka broker setup
- Apache Spark setup
- Required Python libraries (install using `pip install -r requirements.txt` if a `requirements.txt` file is provided).

## Directory Structure

```
miniproject/
├── kafka/
│   └── producer_order.py
├── spark/
│   └── spark_streaming_orders.py
```

## Future Enhancements

- Add more detailed analytics to the Spark Streaming pipeline.
- Implement error handling and retries in the Kafka producer.
- Add unit tests for both components.

## License

This project is licensed under the MIT License.
