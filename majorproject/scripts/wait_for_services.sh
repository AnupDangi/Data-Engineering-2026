#!/bin/bash
# Wait for all Docker services to be healthy

echo "⏳ Waiting for Docker services to be healthy..."

# Function to wait for a service to be healthy
wait_for_service() {
    local service_name=$1
    local timeout=${2:-60}
    local elapsed=0
    
    echo -n "   Waiting for $service_name..."
    while [ $elapsed -lt $timeout ]; do
        if docker ps --filter "name=$service_name" --filter "health=healthy" | grep -q "$service_name"; then
            echo " ✅"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
        echo -n "."
    done
    
    echo " ❌ (timeout after ${timeout}s)"
    return 1
}

# Wait for Zookeeper first
if ! wait_for_service "flowguard-zookeeper" 60; then
    echo "❌ Zookeeper failed to start"
    exit 1
fi

# Give Zookeeper a moment to stabilize
sleep 2

# Wait for all Kafka brokers (in parallel health checks)
kafka_ready=0
for i in 1 2 3; do
    if wait_for_service "flowguard-kafka-$i" 90; then
        kafka_ready=$((kafka_ready + 1))
    fi
done

if [ $kafka_ready -lt 3 ]; then
    echo "⚠️  Warning: Only $kafka_ready/3 Kafka brokers are healthy"
    echo "   The cluster may still work with reduced availability"
else
    echo "✅ All Kafka brokers ready"
fi

# Wait for PostgreSQL
wait_for_service "flowguard-postgres" 30

echo ""
echo "✅ Docker infrastructure ready!"
exit 0
