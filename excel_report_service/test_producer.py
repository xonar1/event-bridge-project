import json
import random
import sys
import time
import uuid

import pika

RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672
QUEUE_NAME = "excel_report_queue"

EVENT_TYPES = ["register"]


def send_message(channel, message: dict) -> None:
    body = json.dumps(message, ensure_ascii=False).encode("utf-8")
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )
    print(f"[SENT] {message}")


def generate_message(seq: int) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "username": f"user_{seq}",
        "email": f"user_{seq}@example.com",
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "details": f"Event #{seq}",
    }


def main() -> None:
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    delay = float(sys.argv[2]) if len(sys.argv) > 2 else 1.0

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
    )
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    print(f"Sending {count} messages with {delay}s delay...")
    for i in range(1, count + 1):
        msg = generate_message(i)
        send_message(channel, msg)
        if i < count:
            time.sleep(delay)

    connection.close()
    print("Done.")


if __name__ == "__main__":
    main()
