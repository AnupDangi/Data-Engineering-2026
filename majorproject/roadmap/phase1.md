for the phase 1: we will implement Single kafka cluster with 3 brokers and many topics which we require.

## Stage 1: Build the streaming of user events from the Events Gateway to kafka.

- Create a FastAPI application that simulates the Events Gateway, generating user events (clicks and orders) and push them to Kafka topics make sure we use both clicks and orders for ads at last.
- Set up a Kafka cluster with 3 brokers and create the necessary topics (e.g., `raw.orders.v1`, `raw.clicks.v1`).
