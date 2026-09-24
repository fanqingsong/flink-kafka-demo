#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka:29092}"
KAFKA_TOPICS="${KAFKA_TOPICS:-/opt/bitnami/kafka/bin/kafka-topics.sh}"
KAFKA_PRODUCER="${KAFKA_PRODUCER:-/opt/bitnami/kafka/bin/kafka-console-producer.sh}"

echo "Waiting for Kafka at ${BOOTSTRAP}..."
for _ in $(seq 1 90); do
  if "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list >/dev/null

for topic in demo.products demo.purchases demo.purchases.enriched demo.running.totals; do
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1
done

existing=0
if end=$("${KAFKA_TOPICS%/kafka-topics.sh}/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic demo.products 2>/dev/null); then
  existing=$(printf '%s\n' "$end" | awk -F: '{sum += $3} END {print sum + 0}')
fi
if [ "$existing" -gt 0 ]; then
  echo "Product catalog already has ${existing} messages, skip seeding."
  exit 0
fi

echo "Publishing product catalog..."
"$KAFKA_PRODUCER" --bootstrap-server "$BOOTSTRAP" --topic demo.products <<'EOF'
{"event_time":"2022-09-13 12:00:00.000000","product_id":"CS06","category":"Classic Smoothies","item":"Blimey Limey","size":"24 oz.","cogs":1.50,"price":4.99,"inventory_level":100,"contains_fruit":true,"contains_veggies":false,"contains_nuts":false,"contains_caffeine":false,"propensity_to_buy":1}
{"event_time":"2022-09-13 12:00:00.000000","product_id":"SF05","category":"Superfoods Smoothies","item":"Caribbean C-Burst","size":"24 oz.","cogs":2.10,"price":5.99,"inventory_level":80,"contains_fruit":true,"contains_veggies":false,"contains_nuts":false,"contains_caffeine":false,"propensity_to_buy":1}
{"event_time":"2022-09-13 12:00:00.000000","product_id":"SF06","category":"Superfoods Smoothies","item":"Get Up and Goji","size":"24 oz.","cogs":2.10,"price":5.99,"inventory_level":90,"contains_fruit":true,"contains_veggies":true,"contains_nuts":false,"contains_caffeine":false,"propensity_to_buy":1}
{"event_time":"2022-09-13 12:00:00.000000","product_id":"SC04","category":"Supercharged Smoothies","item":"Health Nut","size":"24 oz.","cogs":2.70,"price":5.99,"inventory_level":70,"contains_fruit":false,"contains_veggies":false,"contains_nuts":true,"contains_caffeine":false,"propensity_to_buy":1}
EOF

echo "Catalog ready. Send purchases from the console at http://localhost:8088"
