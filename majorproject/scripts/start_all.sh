#!/bin/bash
# Start all FlowGuard services

cd "$(dirname "$0")/.."

echo "========================================="
echo "Starting FlowGuard Services"
echo "========================================="

# 1. Start Docker Infrastructure
echo "1️⃣  Starting Docker infrastructure..."
docker-compose up -d

# Wait for services to be healthy using dedicated script
./scripts/wait_for_services.sh

if [ $? -ne 0 ]; then
    echo "❌ Docker services failed to start properly"
    echo "   Try running: docker-compose down && docker-compose up -d"
    exit 1
fi

# 2. Start Food Catalog Service
echo "2️⃣  Starting Food Catalog Service (port 8001)..."
./scripts/start_catalog.sh > /tmp/catalog.log 2>&1 &
sleep 3

# 3. Start Events Gateway Service
echo "3️⃣  Starting Events Gateway Service (port 8000)..."
PYTHONPATH=$PWD /Users/anupdangi/Desktop/AnupAI/projects/2026/FlowGuard/venv/bin/python \
    -m uvicorn src.services.events_gateway.main:app \
    --host 0.0.0.0 --port 8000 --reload > /tmp/events_gateway.log 2>&1 &
sleep 5

# Check status
echo ""
echo "========================================="
echo "Service Status"
echo "========================================="

if lsof -Pi :8001 -sTCP:LISTEN -t >/dev/null ; then
    echo "✅ Food Catalog Service: http://localhost:8001"
else
    echo "❌ Food Catalog Service FAILED"
fi

if lsof -Pi :8000 -sTCP:LISTEN -t >/dev/null ; then
    echo "✅ Events Gateway Service: http://localhost:8000"
else
    echo "❌ Events Gateway Service FAILED"
fi

if docker ps | grep -q "flowguard-kafka-1"; then
    echo "✅ Kafka Cluster: localhost:19092-19094"
else
    echo "❌ Kafka Cluster FAILED"
fi

if docker ps | grep -q "flowguard-postgres"; then
    echo "✅ PostgreSQL: localhost:5432"
else
    echo "❌ PostgreSQL FAILED"
fi

echo ""
echo "========================================="
echo "Next Steps"
echo "========================================="
echo "• Web UI: cd zomato-web-app && pnpm dev"
echo "• Monitor events: python scripts/monitor_events.py"
echo "• View logs: ./scripts/monitor_services.sh"
echo "• API docs: http://localhost:8000/docs"
