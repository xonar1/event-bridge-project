import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class RabbitMQConfig:
    host: str = field(default_factory=lambda: os.getenv("RABBITMQ_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("RABBITMQ_PORT", "5672")))
    username: str = field(default_factory=lambda: os.getenv("RABBITMQ_DEFAULT_USER", "guest"))
    password: str = field(default_factory=lambda: os.getenv("RABBITMQ_DEFAULT_PASS", "guest"))
    exchange: str = field(default_factory=lambda: os.getenv("RABBITMQ_EXCHANGE", "event_topic_exchange"))
    exchange_type: str = field(default_factory=lambda: os.getenv("RABBITMQ_EXCHANGE_TYPE", "topic"))
    queue: str = field(default_factory=lambda: os.getenv("PDF_QUEUE_NAME", "pdf_ticket_queue"))
    routing_key: str = field(default_factory=lambda: os.getenv("PDF_ROUTING_KEY", "event.registered.*"))
    heartbeat: int = field(default_factory=lambda: int(os.getenv("RABBITMQ_HEARTBEAT", "60")))
    connection_attempts: int = field(default_factory=lambda: int(os.getenv("RABBITMQ_CONN_ATTEMPTS", "3")))
    retry_delay: int = field(default_factory=lambda: int(os.getenv("RABBITMQ_RETRY_DELAY", "5")))


@dataclass(frozen=True)
class AppConfig:
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper())
    service_name: str = "pdf-ticket-service"
    output_dir: str = field(default_factory=lambda: os.getenv("PDF_OUTPUT_DIR", "./generated_tickets"))
    server_host: str = field(default_factory=lambda: os.getenv("PDF_SERVER_HOST", "0.0.0.0"))
    server_port: int = field(default_factory=lambda: int(os.getenv("PDF_SERVER_PORT", "8080")))
    rabbitmq: RabbitMQConfig = field(default_factory=RabbitMQConfig)


settings = AppConfig()

__all__ = ["settings", "AppConfig", "RabbitMQConfig"]
