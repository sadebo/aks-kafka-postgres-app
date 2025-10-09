from fastapi import FastAPI
import pandas as pd
from sqlalchemy import create_engine
from confluent_kafka import Consumer
import os

app = FastAPI(title="Analytics Service")

# Database connection
DB_URI = os.getenv("DATABASE_URL", "postgresql://user:password@postgres:5432/stockdb")
engine = create_engine(DB_URI)

# Kafka config
consumer = Consumer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
    'group.id': 'analytics-service',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([os.getenv("KAFKA_TOPIC", "orders")])

@app.get("/metrics")
def get_metrics():
    with engine.connect() as conn:
        result = pd.read_sql("SELECT product_id, SUM(quantity) as total_sold FROM orders GROUP BY product_id", conn)
    return result.to_dict(orient="records")

@app.get("/healthz")
def health_check():
    return {"status": "ok"}
