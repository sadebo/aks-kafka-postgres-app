from kafka import KafkaConsumer
import os, json

topics = ["orders", "payments", "inventory"]

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "my-cluster-kafka-bootstrap.kafka:9092"),
    group_id="analytics-service",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Analytics Service running...")

for msg in consumer:
    event = msg.value
    print(f"[ANALYTICS] {msg.topic}: {event}")
    # In real case: write to Postgres, Elastic, Data Lake
