from fastapi import FastAPI
from kafka import KafkaProducer
import os, json, uuid

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka-cluster-kafka-bootstrap.kafka:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.post("/order")
def create_order(order: dict):
    order["id"] = str(uuid.uuid4())
    producer.send("orders", order)
    producer.flush()
    return {"status": "Order accepted", "order": order}
