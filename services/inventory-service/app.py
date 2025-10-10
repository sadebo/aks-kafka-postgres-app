from kafka import KafkaConsumer, KafkaProducer
import os, json, psycopg2

consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka-cluster-kafka-bootstrap.kafka:9092"),
    group_id="inventory-service",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka-cluster-kafka-bootstrap.kafka:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

conn = psycopg2.connect(
    dbname=os.getenv("dbname"),
    user=os.getenv("username"),
    password=os.getenv("password"),
    host=os.getenv("host"),
    port=os.getenv("port")
)
cur = conn.cursor()

print("Inventory Service running...")

for msg in consumer:
    order = msg.value
    item = order.get("item", "widget")
    qty = order.get("quantity", 1)

    cur.execute("UPDATE inventory SET stock_level = stock_level - %s WHERE item_name = %s", (qty, item))
    conn.commit()

    cur.execute("SELECT stock_level FROM inventory WHERE item_name = %s", (item,))
    remaining = cur.fetchone()[0]

    update = {"order_id": order["id"], "item": item, "remaining": remaining}
    print(f"Inventory updated: {update}")
    producer.send("inventory", update)
