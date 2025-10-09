from kafka import KafkaConsumer, KafkaProducer
import os, json, random

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "my-cluster-kafka-bootstrap.kafka:9092"),
    group_id="payment-service",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "my-cluster-kafka-bootstrap.kafka:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Payment Service running...")

for msg in consumer:
    order = msg.value
    result = {
        "order_id": order["id"],
        "status": random.choice(["approved", "declined"])
    }
    print(f"Processed payment: {result}")
    producer.send("payments", result)
