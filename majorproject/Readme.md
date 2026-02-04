Only for major project the venv folder is recreated so we dont create any conlict'

User Action (Web App)
↓
[Events Gateway FastAPI]
↓ produces to
[Kafka Cluster 1: raw.orders.v1]
↓ (2 independent consumers)
├──→ [Data Lake Dumper] → S3
│ ↓ (async/scheduled)
│ [Airflow ETL] → reads S3 → produces to [Kafka Cluster 3: recon.overwrite.v1]
│
└──→ [Flink Processor]
↓ produces to
[Kafka Cluster 2: attributed.events.v1]
↓
[Notification Service]
↓
[Redis + User]
