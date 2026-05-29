import json
import os
import sys

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = "console_monitor_queue"
EXCHANGE_NAME = "events"


def send_message(routing_key: str, payload: dict):
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key="#")

    body = json.dumps(payload, ensure_ascii=False)
    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=routing_key,
        body=body.encode("utf-8"),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
        ),
    )
    print(f"[SENT] routing_key={routing_key} -> {body}")
    connection.close()


if __name__ == "__main__":
    test_cases = [
        (
            "event.registered.vip",
            {
                "registration_id": "550e8400-e29b-41d4-a716-446655440000",
                "email": "vip@example.com",
                "event": "VIP Webinar",
            },
        ),
        (
            "event.registered",
            {
                "registration_id": "660e8400-e29b-41d4-a716-446655440001",
                "email": "user@example.com",
                "event": "Standard Meetup",
            },
        ),
        (
            "event.updated",
            {
                "registration_id": "770e8400-e29b-41d4-a716-446655440002",
                "email": "admin@example.com",
                "event": "Conference 2025",
            },
        ),
    ]

    for rk, payload in test_cases:
        send_message(rk, payload)

    print("Готово. Все тестовые сообщения отправлены.")
