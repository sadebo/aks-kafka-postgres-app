from fastapi import FastAPI
from confluent_kafka import Consumer
import os
import threading
import smtplib

app = FastAPI(title="Notification Service")

# Kafka config
consumer = Consumer({
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
    'group.id': 'notification-service',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([os.getenv("KAFKA_TOPIC", "orders")])

# Email config (optional)
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER", "noreply@example.com")
SMTP_PASS = os.getenv("SMTP_PASS", "changeme")

def consume_messages():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        order = msg.value().decode("utf-8")
        print(f"📢 New order event received: {order}")

        # Send fake notification (could be email, SMS, etc.)
        try:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASS)
                message = f"Subject: New Order Notification\n\nNew order received: {order}"
                server.sendmail(SMTP_USER, "customer@example.com", message)
                print("✅ Notification sent")
        except Exception as e:
            print(f"⚠️ Failed to send notification: {e}")

# Run Kafka consumer in background thread
threading.Thread(target=consume_messages, daemon=True).start()

@app.get("/healthz")
def health_check():
    return {"status": "ok"}
