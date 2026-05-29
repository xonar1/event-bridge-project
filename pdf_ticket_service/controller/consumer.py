import json
import logging
import signal
import sys
import time
from typing import Optional

import pika
import pika.exceptions

from config import settings
from model.eventReger import RegistrationEvent
from model.service import TicketService

logger = logging.getLogger(settings.service_name)


class PdfTicketConsumer:
    def __init__(self) -> None:
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self._closing = False
        self._consumer_tag: Optional[str] = None
        self._service = TicketService(output_dir=settings.output_dir)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _connect(self) -> pika.BlockingConnection:
        credentials = pika.PlainCredentials(settings.rabbitmq.username, settings.rabbitmq.password)
        params = pika.ConnectionParameters(
            host=settings.rabbitmq.host,
            port=settings.rabbitmq.port,
            credentials=credentials,
            heartbeat=settings.rabbitmq.heartbeat,
            connection_attempts=settings.rabbitmq.connection_attempts,
            retry_delay=settings.rabbitmq.retry_delay,
        )
        logger.info("Connecting to RabbitMQ at %s:%s …", settings.rabbitmq.host, settings.rabbitmq.port)
        return pika.BlockingConnection(params)

    def _declare_queue(self) -> None:
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=settings.rabbitmq.exchange,
            exchange_type=settings.rabbitmq.exchange_type,
            durable=True,
        )
        self._channel.queue_declare(queue=settings.rabbitmq.queue, durable=True)
        self._channel.queue_bind(
            queue=settings.rabbitmq.queue,
            exchange=settings.rabbitmq.exchange,
            routing_key=settings.rabbitmq.routing_key,
        )
        self._channel.basic_qos(prefetch_count=1)
        logger.info(
            "Queue '%s' bound to exchange '%s' with routing '%s'",
            settings.rabbitmq.queue,
            settings.rabbitmq.exchange,
            settings.rabbitmq.routing_key,
        )

    def _on_message(
        self,
        channel: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        _properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        delivery_tag = method.delivery_tag
        try:
            data = json.loads(body)
            event = RegistrationEvent(**data)
            result = self._service.process_event(event)
            if result.success:
                logger.info("PDF ticket generated: %s", result.filename)
                channel.basic_ack(delivery_tag=delivery_tag)
            else:
                logger.warning("Ticket rejected for %s: %s", event.registration_id, result.error)
                channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON: %s", exc)
            channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
        except Exception:
            logger.exception("Unexpected error processing message %s", delivery_tag)
            channel.basic_nack(delivery_tag=delivery_tag, requeue=False)

    def run(self) -> None:
        while not self._closing:
            try:
                self._connection = self._connect()
                self._declare_queue()
                self._consumer_tag = self._channel.basic_consume(
                    queue=settings.rabbitmq.queue,
                    on_message_callback=self._on_message,
                    auto_ack=False,
                )
                logger.info(
                    "Consumer started — waiting for messages on '%s'. Press Ctrl+C to stop.",
                    settings.rabbitmq.queue,
                )
                self._channel.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker:
                logger.warning("Connection closed by broker — reconnecting …")
                continue
            except pika.exceptions.AMQPChannelError as exc:
                logger.critical("Channel error: %s — shutting down", exc)
                break
            except pika.exceptions.AMQPConnectionError:
                logger.warning("Connection lost — reconnecting …")
                self._reconnect_delay()
                continue
            except KeyboardInterrupt:
                self.stop()
                break

    def _reconnect_delay(self) -> None:
        delay = min(settings.rabbitmq.retry_delay * 2, 30)
        logger.info("Reconnecting in %s seconds …", delay)
        time.sleep(delay)

    def stop(self) -> None:
        if self._closing:
            return
        self._closing = True
        logger.info("Initiating graceful shutdown …")
        if self._channel and self._consumer_tag:
            try:
                self._channel.basic_cancel(self._consumer_tag)
            except Exception as exc:
                logger.warning("Error during consumer cancel: %s", exc)
        if self._connection and self._connection.is_open:
            try:
                self._connection.close()
            except Exception as exc:
                logger.warning("Error closing connection: %s", exc)
        logger.info("PDF ticket consumer stopped.")

    def _signal_handler(self, signum: int, _frame) -> None:
        signame = signal.Signals(signum).name
        logger.info("Received %s — shutting down gracefully …", signame)
        self.stop()


def run_consumer() -> None:
    consumer = PdfTicketConsumer()
    consumer.run()
