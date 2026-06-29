#!/usr/bin/env bash
# =============================================================================
# 02-create-topics.sh
# Creates the demo Kafka topics used by the test data producer.
# Run this after `docker compose up -d` and before producing test data.
# =============================================================================

set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION:-1}"
RETENTION_MS="${RETENTION_MS:-604800000}"   # 7 days

TOPICS=(
    "orders"
    "payments"
    "customers"
    "inventory"
)

echo "Creating Kafka topics on broker: $BOOTSTRAP"

for TOPIC in "${TOPICS[@]}"; do
    echo "  Creating topic: $TOPIC"
    docker exec "$KAFKA_CONTAINER" \
        kafka-topics \
        --bootstrap-server "$BOOTSTRAP" \
        --create \
        --if-not-exists \
        --topic "$TOPIC" \
        --partitions "$PARTITIONS" \
        --replication-factor "$REPLICATION" \
        --config "retention.ms=$RETENTION_MS"
done

echo ""
echo "Listing all topics:"
docker exec "$KAFKA_CONTAINER" \
    kafka-topics --bootstrap-server "$BOOTSTRAP" --list

echo ""
echo "Topics created successfully."
