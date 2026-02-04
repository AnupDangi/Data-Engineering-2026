# FlowGuard - Phase 1: Events Gateway

## 📋 Phase 1 Completed ✅

Successfully implemented the Events Gateway service with Kafka integration!

### **What's Working:**
- ✅ Kafka cluster (3 brokers + Zookeeper)
- ✅ Kafka UI for monitoring (port 8080)
- ✅ FastAPI Events Gateway (port 8000)
- ✅ Order events endpoint
- ✅ Click/impression events endpoint
- ✅ Health checks and metrics
- ✅ Proper error handling and logging

---

## 🚀 Quick Start Guide

### **1. Start Kafka Cluster**
```bash
cd /Users/anupdangi/Desktop/AnupAI/projects/2026/FlowGuard/majorproject

# Start all services (Kafka, Zookeeper, Kafka UI)
docker-compose up -d

# Check health
./scripts/health_check.sh

# View Kafka UI
open http://localhost:8080
```

### **2. Install Python Dependencies**
```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### **3. Start Events Gateway**
```bash
# Method 1: Using CLI
python src/main.py start-gateway

# Method 2: Direct uvicorn
python src/services/events_gateway/main.py

# Method 3: With custom port
python src/main.py start-gateway --port 8001 --reload
```

### **4. Test the API**
```bash
# Check if service is running
curl http://localhost:8000/

# Health check
curl http://localhost:8000/health

# Submit an order event
curl -X POST http://localhost:8000/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "test_001",
    "user_id": 1001,
    "order_id": "ORD-001",
    "item_id": 501,
    "item_name": "Biryani",
    "price": 299.99
  }'

# Submit a click event
curl -X POST http://localhost:8000/api/v1/clicks \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "test_002",
    "user_id": 1001,
    "ad_id": "ad_123",
    "is_click": true,
    "ad_type": "banner"
  }'
```

### **5. Verify in Kafka UI**
1. Open http://localhost:8080
2. Go to "Topics"
3. Check `raw.orders.v1` and `raw.clicks.v1`
4. View messages in each topic

---

## 📁 Project Structure

```
majorproject/
├── src/
│   ├── services/events_gateway/      # FastAPI service
│   │   ├── main.py                   # App entry point
│   │   ├── routers/
│   │   │   ├── orders.py             # Order events API
│   │   │   └── clicks.py             # Click events API
│   │   └── producers/
│   │       └── kafka_producer.py     # Kafka producer wrapper
│   ├── shared/
│   │   ├── kafka/
│   │   │   ├── config.py             # Kafka configuration
│   │   │   └── topics.py             # Topic definitions
│   │   └── schemas/
│   │       └── events.py             # Event schemas
│   └── main.py                        # CLI orchestrator
├── docker-compose.yml                 # Kafka infrastructure
├── scripts/
│   ├── init_kafka_topics.sh          # Topic initialization
│   └── health_check.sh               # Health checks
└── config/
    └── kafka/topics.yaml              # Topic configurations
```

---

## 🎯 API Endpoints

### **Events Gateway (Port 8000)**

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | Service information |
| GET | `/health` | Health check with Kafka status |
| GET | `/metrics` | Producer statistics |
| GET | `/docs` | Interactive API documentation |
| POST | `/api/v1/orders` | Submit order event |
| POST | `/api/v1/clicks` | Submit click/impression event |
| POST | `/api/v1/clicks/impression` | Submit impression event |

### **Kafka UI (Port 8080)**
- Dashboard: http://localhost:8080
- Topics management
- Message browser
- Consumer group monitoring

---

## 🔧 CLI Commands

```bash
# Start Events Gateway
python src/main.py start-gateway

# Health check
python src/main.py health-check

# Simulate events (placeholder)
python src/main.py simulate --rate 5 --duration 10
```

---

## 📊 Kafka Topics

| Topic Name | Partitions | Replication | Retention | Purpose |
|------------|------------|-------------|-----------|---------|
| `raw.orders.v1` | 3 | 2 | 7 days | Order events from users |
| `raw.clicks.v1` | 6 | 2 | 2 days | Click/impression events for ads |

---

## ✅ Verification Checklist

- [ ] Kafka cluster running (3 brokers)
- [ ] Zookeeper running
- [ ] Kafka UI accessible at http://localhost:8080
- [ ] Topics created (raw.orders.v1, raw.clicks.v1)
- [ ] Events Gateway running at http://localhost:8000
- [ ] `/health` endpoint returns healthy status
- [ ] Can submit order events successfully
- [ ] Can submit click events successfully
- [ ] Messages visible in Kafka UI

---

## 🐛 Troubleshooting

### Kafka not starting?
```bash
# Check logs
docker-compose logs kafka-broker-1

# Restart services
docker-compose restart
```

### Events Gateway can't connect to Kafka?
```bash
# Check if Kafka is running
docker ps | grep kafka

# Verify environment variables
cat .env | grep KAFKA

# Test connection
python src/main.py health-check
```

### Port already in use?
```bash
# Check what's using port 8000
lsof -i :8000

# Start on different port
python src/main.py start-gateway --port 8001
```

---

## 📝 Git Commits Summary

Phase 1 completed with **8 professional commits**:

1. ✅ Project structure setup
2. ✅ Configuration files
3. ✅ Kafka infrastructure
4. ✅ Topic initialization scripts
5. ✅ Shared Kafka components
6. ✅ Producer wrapper
7. ✅ FastAPI routers
8. ✅ Main application & CLI

---

## 🚀 Next Steps (Phase 2)

1. **Data Lake Dumper**: Consume from Kafka → Write to S3/MinIO
2. **Flink Processing**: Real-time attribution job
3. **Event Simulators**: Automated load testing
4. **Monitoring**: Prometheus + Grafana
5. **Tests**: Unit and integration tests

---

## 📚 Documentation

- API Docs: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
- Kafka UI: http://localhost:8080

---

**Status:** ✅ Phase 1 Complete - Ready for Testing!
