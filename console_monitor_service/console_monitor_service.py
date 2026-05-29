import json
import os
import sys

import pika
from rich.console import Console
from rich.text import Text

console = Console()

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = "console_monitor_queue"
EXCHANGE_NAME = "events"

EVENT_COLORS = {
    "event.registered.vip": "bold red",
    "event.registered": "bold green",
    "event.updated": "bold yellow",
    "event.deleted": "bold magenta",
    "event.confirmed": "bold cyan",
    "user.created": "bold blue",
    "user.updated": "bold white",
    "default": "bold white",
}


def get_color_by_routing_key(routing_key: str) -> str:
    return EVENT_COLORS.get(routing_key, EVENT_COLORS["default"])


def format_message(routing_key: str, body: dict) -> Text:
    color = get_color_by_routing_key(routing_key)
    registration_id = body.get("registration_id", "N/A")

    user = body.get("user", "")
    email = body.get("email", "")
    event = body.get("event", "")

    if user and email:
        data_part = f"User: {email} registered for {event}" if event else f"User: {email}"
    elif email:
        data_part = f"User: {email} registered for {event}" if event else f"User: {email}"
    else:
        data_part = str(body)

    text = Text()
    text.append("[MONITOR] ", style="bold dim")
    text.append(f"[{routing_key}] ", style=color)
    text.append(f"ID: {registration_id} - ", style="bold white")
    text.append(data_part, style="white")
    return text


def on_message(ch, method, properties, body):
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        console.print("[MONITOR] ", style="bold dim", end="")
        console.print(f"[raw] {body.decode('utf-8', errors='replace')}", style="bold white")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    text = format_message(method.routing_key, data)
    console.print(text)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    try:
        connection = pika.BlockingConnection(parameters)
    except pika.exceptions.AMQPConnectionError as exc:
        console.print(f"[ERROR] Не удалось подключиться к RabbitMQ: {exc}", style="bold red")
        sys.exit(1)

    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key="#")
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message)

    console.print(f"[INFO] Сервис мониторинга запущён. Очередь: {QUEUE_NAME}", style="bold green")
    console.print("[INFO] Ожидание сообщений... Нажмите Ctrl+C для выхода.\n", style="dim")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        console.print("\n[INFO] Завершение работы...", style="bold yellow")
        channel.stop_consuming()
        connection.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
