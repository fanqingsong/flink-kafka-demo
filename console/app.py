import json
import os
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, Field

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC_PURCHASES = "demo.purchases"
TOPIC_TOTALS = "demo.running.totals"
TOPIC_ENRICHED = "demo.purchases.enriched"

PRODUCTS = [
    {"product_id": "CS06", "item": "Blimey Limey", "category": "Classic Smoothies", "price": "4.99"},
    {"product_id": "SF05", "item": "Caribbean C-Burst", "category": "Superfoods Smoothies", "price": "5.99"},
    {"product_id": "SF06", "item": "Get Up and Goji", "category": "Superfoods Smoothies", "price": "5.99"},
    {"product_id": "SC04", "item": "Health Nut", "category": "Supercharged Smoothies", "price": "5.99"},
]
PRODUCT_BY_ID = {item["product_id"]: item for item in PRODUCTS}

state_lock = threading.Lock()
totals = {}
enriched = deque(maxlen=40)
purchases = deque(maxlen=40)
consumer_ready = False
consumer_error = None
session_sent = 0
producer = None
producer_lock = threading.Lock()


class PurchaseRequest(BaseModel):
    product_id: str
    quantity: int = Field(ge=1, le=99)
    is_member: bool = False


def money(value: Decimal) -> float:
    quantized = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(format(quantized, "f"))


def build_purchase(body: PurchaseRequest) -> dict:
    product = PRODUCT_BY_ID.get(body.product_id)
    if product is None:
        raise HTTPException(status_code=400, detail="unknown product_id")
    price = Decimal(product["price"])
    discount = Decimal("0.10") if body.is_member else Decimal("0")
    total = price * body.quantity * (Decimal("1") - discount)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000000")
    return {
        "transaction_time": now,
        "transaction_id": f"{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}",
        "product_id": product["product_id"],
        "price": money(price),
        "quantity": body.quantity,
        "is_member": body.is_member,
        "member_discount": money(discount),
        "add_supplements": False,
        "supplement_price": 0.0,
        "total_purchase": money(total),
        "product_name": product["item"],
    }


def get_producer() -> KafkaProducer:
    global producer
    with producer_lock:
        if producer is None:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks="all",
                retries=3,
                request_timeout_ms=10000,
            )
        return producer


def remember(topic: str, value: dict) -> None:
    if not isinstance(value, dict):
        return
    with state_lock:
        if topic == TOPIC_TOTALS:
            product_id = value.get("product_id")
            if product_id:
                totals[product_id] = value
        elif topic == TOPIC_ENRICHED:
            enriched.appendleft(value)
        elif topic == TOPIC_PURCHASES:
            purchases.appendleft(value)


def consume_loop() -> None:
    global consumer_ready, consumer_error
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                TOPIC_PURCHASES,
                TOPIC_TOTALS,
                TOPIC_ENRICHED,
                bootstrap_servers=BOOTSTRAP,
                group_id=f"console-{uuid.uuid4()}",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            )
            consumer_ready = True
            consumer_error = None
            while True:
                batches = consumer.poll(timeout_ms=1000, max_records=500)
                for _tp, messages in batches.items():
                    for message in messages:
                        remember(message.topic, message.value)
        except Exception as exc:
            consumer_ready = False
            consumer_error = str(exc)
            time.sleep(2)
        finally:
            if consumer is not None:
                consumer.close()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    threading.Thread(target=consume_loop, name="kafka-tail", daemon=True).start()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
def index():
    return FileResponse("static/index.html")


@app.get("/api/state")
def snapshot():
    with state_lock:
        total_rows = []
        for product_id, row in totals.items():
            product = PRODUCT_BY_ID.get(product_id, {})
            total_rows.append({
                "product_id": product_id,
                "product_name": product.get("item", ""),
                "category": product.get("category", ""),
                "transactions": row.get("transactions"),
                "quantities": row.get("quantities"),
                "sales": row.get("sales"),
                "event_time": row.get("event_time"),
            })
        total_rows.sort(key=lambda item: item["product_id"])
        return {
            "ready": consumer_ready,
            "error": consumer_error,
            "session_sent": session_sent,
            "products": PRODUCTS,
            "totals": total_rows,
            "purchases": list(purchases),
            "enriched": list(enriched),
        }


@app.post("/api/purchases")
def send_purchase(body: PurchaseRequest):
    global session_sent
    record = build_purchase(body)
    payload = {key: value for key, value in record.items() if key != "product_name"}
    try:
        get_producer().send(TOPIC_PURCHASES, payload).get(timeout=10)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"kafka produce failed: {exc}") from exc
    with state_lock:
        session_sent += 1
    return record
