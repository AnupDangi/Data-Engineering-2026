from producer.csv_reader import stream_csv
from producer.rate_controller import rate_limit
from producer.event_builder import build_event


CSV_PATH = "./data/ZomatoDataset.csv"
BATCH_SIZE = 100
INTERVAL_SECONDS = 1


def main():
    csv_stream = stream_csv(CSV_PATH)
    controlled_stream = rate_limit(
        csv_stream,
        batch_size=BATCH_SIZE,
        interval_seconds=INTERVAL_SECONDS
    )

    total_events = 0

    for batch in controlled_stream:
        events = [build_event(row) for row in batch]

        # For now, just print summary (not full payload)
        print(f"Emitted batch of {len(events)} events")
        print(f"Sample event_id: {events[0]['event_id']}")
        print("-" * 50)

        total_events += len(events)

    print(f"Streaming completed. Total events emitted: {total_events}")


if __name__ == "__main__":
    main()
