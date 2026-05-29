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
    queue: str = field(default_factory=lambda: os.getenv("SLACK_QUEUE_NAME", "registration_events"))
    exchange: str = field(default_factory=lambda: os.getenv("RABBITMQ_EXCHANGE", "event_topic_exchange"))
    exchange_type: str = field(default_factory=lambda: os.getenv("RABBITMQ_EXCHANGE_TYPE", "topic"))
    routing_key: str = field(default_factory=lambda: os.getenv("RABBITMQ_ROUTING_KEY", "event.registered.*"))
    heartbeat: int = field(default_factory=lambda: int(os.getenv("RABBITMQ_HEARTBEAT", "60")))
    connection_attempts: int = field(default_factory=lambda: int(os.getenv("RABBITMQ_CONN_ATTEMPTS", "3")))
    retry_delay: int = field(default_factory=lambda: int(os.getenv("RABBITMQ_RETRY_DELAY", "5")))

    @property
    def connection_parameters(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "credentials": (self.username, self.password),
            "heartbeat": self.heartbeat,
            "connection_attempts": self.connection_attempts,
            "retry_delay": self.retry_delay,
        }


@dataclass(frozen=True)
class SlackConfig:
    webhook_url: str = field(
        default_factory=lambda: os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/your/webhook/url")
    )
    request_timeout: int = field(default_factory=lambda: int(os.getenv("SLACK_REQUEST_TIMEOUT", "10")))


@dataclass(frozen=True)
class PdfServiceConfig:
    base_url: str = field(default_factory=lambda: os.getenv("PDF_SERVICE_BASE_URL", "http://localhost:8080"))
    check_timeout: int = field(default_factory=lambda: int(os.getenv("PDF_SERVICE_CHECK_TIMEOUT", "3")))


@dataclass(frozen=True)
class AppConfig:
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper())
    service_name: str = "slack-integration"
    rabbitmq: RabbitMQConfig = field(default_factory=RabbitMQConfig)
    slack: SlackConfig = field(default_factory=SlackConfig)
    pdf_service: PdfServiceConfig = field(default_factory=PdfServiceConfig)


settings = AppConfig()

__all__ = ["settings", "AppConfig", "RabbitMQConfig", "SlackConfig", "PdfServiceConfig"]
