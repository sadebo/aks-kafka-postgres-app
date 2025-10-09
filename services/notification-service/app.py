from kafka import KafkaConsumer
import os, json

topics = ["orders", "payments"]

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "my-cluster-kafka-bootstrap.kafka:9092"),
    group_id="notification-service",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Notification Service running...")

for msg in consumer:
    event = msg.value
    print(f"[NOTIFY] Event from {msg.topic}: {event}")
