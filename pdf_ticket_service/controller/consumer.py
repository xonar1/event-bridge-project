import json
import pika
from model.eventReger import RegistrationEvent
from model.service import TicketService


EXCHANGE = "event_topic_exchange"
QUEUE = "pdf_ticket_queue"
ROUTING_PATTERN = "event.registered.*"


def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        event = RegistrationEvent(**data)
        service = TicketService()
        result = service.process_event(event)
        if result.success:
            print(f"[consumer] PDF ticket generated: {result.filename}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            print(f"[consumer] Rejected: {result.error}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as exc:
        print(f"[consumer] Error: {exc}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def run_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_PATTERN)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE, on_message_callback=callback, auto_ack=False)
    print(f"[consumer] Waiting for events on '{QUEUE}' (exchange={EXCHANGE})")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        connection.close()
