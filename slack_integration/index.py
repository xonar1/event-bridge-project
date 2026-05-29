import json
import logging
import signal
import sys
import time
from typing import Any, Dict, Optional

import pika
import pika.credentials
import pika.exceptions
import requests

from config import settings
from eventHandler import format_slack_message
from message_contract import RegistrationEvent

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    stream=sys.stdout,
)
logger = logging.getLogger(settings.service_name)


def resolve_ticket_url(registration_id: str) -> Optional[str]:
    try:
        url = f"{settings.pdf_service.base_url}/t/{registration_id}"
        resp = requests.head(url, timeout=settings.pdf_service.check_timeout)
        if resp.ok:
            logger.debug("PDF ticket exists for %s", registration_id)
            return url
        logger.debug("PDF ticket not found for %s (HTTP %s)", registration_id, resp.status_code)
        return None
    except requests.RequestException as exc:
        logger.warning("PDF service unreachable for %s: %s", registration_id, exc)
        return None


def post_to_slack(payload: Dict[str, Any]) -> None:
    response = requests.post(
        settings.slack.webhook_url,
        json=payload,
        timeout=settings.slack.request_timeout,
    )
    if not response.ok:
        logger.error("Slack webhook returned %s: %s", response.status_code, response.text[:500])
        response.raise_for_status()
    logger.info("Message delivered to Slack (HTTP %s)", response.status_code)


def send_registration_notification(event: RegistrationEvent) -> None:
    logger.info(
        "Processing registration | id=%s event=%s user=%s vip=%s",
        event.registration_id,
        event.event_name,
        event.user_email,
        event.is_vip,
    )
    ticket_url = resolve_ticket_url(event.registration_id)
    payload = format_slack_message(event, ticket_url)
    post_to_slack(payload)


class SlackConsumer:
    def __init__(self) -> None:
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self._closing = False
        self._consumer_tag: Optional[str] = None
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
            data: Dict[str, Any] = json.loads(body)
            event = RegistrationEvent(**data)
            send_registration_notification(event)
            channel.basic_ack(delivery_tag=delivery_tag)
            logger.debug("Acked message %s", delivery_tag)
        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON in message body: %s", exc)
            channel.basic_nack(delivery_tag=delivery_tag, requeue=False)
        except (requests.RequestException, ConnectionError) as exc:
            logger.warning("Slack delivery failed for message %s: %s — retrying", delivery_tag, exc)
            channel.basic_nack(delivery_tag=delivery_tag, requeue=True)
        except Exception:
            logger.exception("Unexpected error processing message %s — dead-lettering", delivery_tag)
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
                logger.debug("Consumer tag %s cancelled", self._consumer_tag)
            except Exception as exc:
                logger.warning("Error during consumer cancel: %s", exc)
        if self._connection and self._connection.is_open:
            try:
                self._connection.close()
                logger.debug("RabbitMQ connection closed.")
            except Exception as exc:
                logger.warning("Error closing connection: %s", exc)
        logger.info("Slack integration service stopped.")

    def _signal_handler(self, signum: int, _frame) -> None:
        signame = signal.Signals(signum).name
        logger.info("Received %s — shutting down gracefully …", signame)
        self.stop()


def lambda_handler(event: Dict[str, Any], context: Optional[Any]) -> Dict[str, Any]:
    try:
        if isinstance(event.get("body"), str):
            data = json.loads(event["body"])
        else:
            data = event
        registration = RegistrationEvent(**data)
        send_registration_notification(registration)
        return {"statusCode": 200, "body": "Message sent to Slack successfully!"}
    except Exception as exc:
        logger.exception("Lambda handler failed")
        return {"statusCode": 500, "body": f"Error: {exc}"}


def main() -> None:
    consumer = SlackConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
