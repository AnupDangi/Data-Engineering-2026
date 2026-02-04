majorproject/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .env.example
├── .gitignore
│
├── docs/
│ ├── architecture.md
│ ├── event_schemas.md
│ ├── kafka_clusters.md
│ └── runbook.md
│
├── infra/
│ ├── kafka/
│ │ ├── raw-cluster/
│ │ ├── realtime-cluster/
│ │ └── billing-cluster/
│ │
│ ├── flink/
│ ├── redis/
│ └── s3/
│
├── src/
│ ├── main.py
│ │
│ ├── config/
│ │ ├── settings.py
│ │ ├── kafka.yaml
│ │ └── logging.yaml
│ │
│ ├── common/
│ │ ├── models/
│ │ │ └── events.py
│ │ ├── kafka/
│ │ │ ├── producer.py
│ │ │ └── consumer.py
│ │ ├── serialization/
│ │ └── utils/
│ │
│ ├── services/
│ │ ├── events_gateway/
│ │ │ ├── api.py
│ │ │ ├── handlers.py
│ │ │ └── publisher.py
│ │ │
│ │ ├── orders/
│ │ │ ├── api.py
│ │ │ ├── service.py
│ │ │ ├── events.py
│ │ │ └── repository.py
│ │ │
│ │ └── ads/
│ │ └── events.py
│ │
│ ├── real_time/
│ │ ├── flink_jobs/
│ │ │ ├── attribution.sql
│ │ │ └── sinks.py
│ │ │
│ │ └── consumers/
│ │ └── billing_consumer.py
│ │
│ ├── batch_processing/
│ │ ├── etl/
│ │ │ ├── recon_job.py
│ │ │ └── validators.py
│ │ └── data_lake/
│ │ └── s3_reader.py
│ │
│ └── web_app/
│ └── user_activity.html
│
└── tests/
├── unit/
└── integration/
