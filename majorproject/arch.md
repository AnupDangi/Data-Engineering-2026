majorproject/
├── src/
│ ├── services/events_gateway/ ← Phase 1 focus
│ │ ├── routers/
│ │ ├── producers/
│ │ └── simulators/
│ ├── shared/
│ │ ├── kafka/
│ │ └── schemas/
│ └── main.py
├── config/
│ ├── kafka/topics.yaml
│ └── settings.yaml
├── scripts/
│ ├── init_kafka_topics.sh
│ └── health_check.sh
├── docker-compose.yml ← 3 Kafka brokers ready
└── .env.example
