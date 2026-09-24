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

echo "Publishing product catalog..."
"$KAFKA_PRODUCER" --bootstrap-server "$BOOTSTRAP" --topic demo.products <<'EOF'
{"event_time":"2022-09-13 12:00:00.000000","product_id":"CS06","category":"Classic Smoothies","item":"Blimey Limey","size":"24 oz.","cogs":1.50,"price":4.99,"inventory_level":100,"contains_fruit":true,"contains_veggies":false,"contains_nuts":false,"contains_caffeine":false,"propensity_to_buy":1}
{"event_time":"2022-09-13 12:00:00.000000","product_id":"SF05","category":"Superfoods Smoothies","item":"Caribbean C-Burst","size":"24 oz.","cogs":2.10,"price":5.99,"inventory_level":80,"contains_fruit":true,"contains_veggies":false,"contains_nuts":false,"contains_caffeine":false,"propensity_to_buy":1}
{"event_time":"2022-09-13 12:00:00.000000","product_id":"SF06","category":"Superfoods Smoothies","item":"Get Up and Goji","size":"24 oz.","cogs":2.10,"price":5.99,"inventory_level":90,"contains_fruit":true,"contains_veggies":true,"contains_nuts":false,"contains_caffeine":false,"propensity_to_buy":1}
{"event_time":"2022-09-13 12:00:00.000000","product_id":"SC04","category":"Supercharged Smoothies","item":"Health Nut","size":"24 oz.","cogs":2.70,"price":5.99,"inventory_level":70,"contains_fruit":false,"contains_veggies":false,"contains_nuts":true,"contains_caffeine":false,"propensity_to_buy":1}
EOF

echo "Publishing purchases..."
products=(CS06 SF05 SF06 SC04)
prices=(4.99 5.99 5.99 5.99)
i=0
while true; do
  idx=$((i % 4))
  product_id="${products[$idx]}"
  price="${prices[$idx]}"
  ts="$(date -u +"%Y-%m-%d %H:%M:%S.000000")"
  txn="${i}-$(date +%s)"
  printf '{"transaction_time":"%s","transaction_id":"%s","product_id":"%s","price":%s,"quantity":1,"is_member":false,"member_discount":0.0,"add_supplements":false,"supplement_price":0.0,"total_purchase":%s}\n' \
    "$ts" "$txn" "$product_id" "$price" "$price"
  i=$((i + 1))
  sleep 1
done | "$KAFKA_PRODUCER" --bootstrap-server "$BOOTSTRAP" --topic demo.purchases
