import json
import logging
import subprocess
import threading
import time

import pika
import redis

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logging.getLogger("pika").setLevel(logging.WARNING)


class AnalyticsService:
    def __init__(self):
        self.redis_client = None
        self.rabbit_connection = None
        self.rabbit_channel = None
        self._stop_event = threading.Event()
        self._stats_thread = None

    #  Подключения

    @staticmethod
    def _ensure_redis_container():
        try:
            r = redis.Redis(
                host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB
            )
            r.ping()
            r.close()
            return
        except redis.ConnectionError:
            logger.info("Redis не доступен, пытаемся запустить контейнер...")

        try:
            # Проверяем, есть ли остановленный контейнер
            result = subprocess.run(
                ["docker", "ps", "-a", "-q", "-f", "name=eventbridge_redis"],
                capture_output=True,
                text=True,
            )
            if result.stdout.strip():
                logger.info("Запускаю существующий контейнер eventbridge_redis...")
                subprocess.run(["docker", "start", "eventbridge_redis"], check=True)
            else:
                logger.info("Создаю и запускаю контейнер eventbridge_redis...")
                subprocess.run(
                    [
                        "docker",
                        "run",
                        "-d",
                        "--name",
                        "eventbridge_redis",
                        "-p",
                        f"{config.REDIS_PORT}:6379",
                        "redis:7-alpine",
                    ],
                    check=True,
                )
            # Ждём, пока Redis поднимется
            for _ in range(15):
                try:
                    r = redis.Redis(
                        host=config.REDIS_HOST,
                        port=config.REDIS_PORT,
                        db=config.REDIS_DB,
                    )
                    r.ping()
                    r.close()
                    logger.info("Redis контейнер запущен и готов")
                    return
                except redis.ConnectionError:
                    time.sleep(1)
            raise RuntimeError("Redis не поднялся в течение 15 секунд")
        except FileNotFoundError:
            raise RuntimeError(
                "Docker не установлен или не добавлен в PATH. Запустите Redis вручную."
            )

    @staticmethod
    def _ensure_rabbitmq_available():
        import socket

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(3)
            sock.connect((config.RABBITMQ_HOST, config.RABBITMQ_PORT))
            sock.close()
        except (socket.timeout, ConnectionRefusedError, OSError):
            raise RuntimeError(
                f"RabbitMQ не доступен на {config.RABBITMQ_HOST}:{config.RABBITMQ_PORT}. "
                f"Запустите: docker run -d --name eventbridge_rabbitmq "
                f"-p 5672:5672 -p 15672:15672 rabbitmq:3-management"
            )

    def _connect_redis(self):
        self._ensure_redis_container()
        self.redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True,
        )
        self.redis_client.ping()
        logger.info("Подключено к Redis: %s:%s", config.REDIS_HOST, config.REDIS_PORT)

    def _connect_rabbitmq(self):
        self._ensure_rabbitmq_available()
        credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=config.RABBITMQ_HOST,
            port=config.RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )
        self.rabbit_connection = pika.BlockingConnection(parameters)
        self.rabbit_channel = self.rabbit_connection.channel()

        # Объявляем очередь
        self.rabbit_channel.queue_declare(queue=config.RABBITMQ_QUEUE, durable=True)

        # Объявляем exchange и связываем с очередью
        self.rabbit_channel.exchange_declare(
            exchange="event_topic_exchange",
            exchange_type="topic",
            durable=True,
        )
        self.rabbit_channel.queue_bind(
            queue=config.RABBITMQ_QUEUE,
            exchange="event_topic_exchange",
            routing_key="event.registered.#",
        )

        logger.info(
            "Подключено к RabbitMQ: %s:%s, очередь: %s",
            config.RABBITMQ_HOST,
            config.RABBITMQ_PORT,
            config.RABBITMQ_QUEUE,
        )

    #  Обработка сообщений

    def _process_message(self, body):
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("Не удалось декодировать тело сообщения: %s", body)
            return

        event_name = data.get("event_name")
        is_vip = data.get("is_vip", False)

        pipe = self.redis_client.pipeline()

        # 1 Общий счётчик регистраций
        pipe.incr("total_registrations")

        # 2 Счётчик VIP-регистраций
        if is_vip:
            pipe.incr("vip_registrations")

        # 3 Счётчик по каждому мероприятию
        if event_name:
            pipe.hincrby("registrations_per_event", event_name, 1)
        else:
            logger.warning("В сообщении отсутствует 'event_name': %s", data)

        pipe.execute()
        logger.debug(
            "Обработана регистрация: event=%s, is_vip=%s",
            event_name,
            is_vip,
        )

    def _on_message(self, ch, method, properties, body):
        logger.debug("Получено сообщение: %s", body)
        self._process_message(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    # Периодический вывод статистики 

    def _print_stats(self):
        while not self._stop_event.is_set():
            self._stop_event.wait(config.STATS_INTERVAL)
            if self._stop_event.is_set():
                break

            try:
                total = self.redis_client.get("total_registrations") or "0"
                vip = self.redis_client.get("vip_registrations") or "0"
                per_event = self.redis_client.hgetall("registrations_per_event")

                logger.info("=== Текущая статистика ===")
                logger.info("Всего регистраций: %s", total)
                logger.info("VIP регистраций: %s", vip)
                if per_event:
                    logger.info("Регистрации по мероприятиям:")
                    for ev, count in per_event.items():
                        logger.info("  %s: %s", ev, count)
                else:
                    logger.info("Регистрации по мероприятиям: (нет)")
                logger.info("==========================")
            except Exception as exc:
                logger.error("Не удалось получить статистику: %s", exc)

    #  Запуск / остановка

    def _run_consumer(self):
        self.rabbit_channel.basic_qos(prefetch_count=1)
        self.rabbit_channel.basic_consume(
            queue=config.RABBITMQ_QUEUE,
            on_message_callback=self._on_message,
        )
        logger.info("Сервис аналитики запущен. Ожидание сообщений...")
        self.rabbit_channel.start_consuming()

    def start(self):
        self._connect_redis()

        self._stats_thread = threading.Thread(target=self._print_stats, daemon=True)
        self._stats_thread.start()

        attempt = 0
        while not self._stop_event.is_set():
            try:
                self._connect_rabbitmq()
                attempt = 0  # сброс счётчика при успешном подключении
                self._run_consumer()
            except pika.exceptions.AMQPConnectionError as exc:
                logger.error("Потеряно соединение с RabbitMQ: %s", exc)
            except pika.exceptions.ConnectionClosedByBroker as exc:
                logger.error("RabbitMQ закрыл соединение: код=%s", exc.reply_code)
            except pika.exceptions.AMQPChannelError as exc:
                logger.error("Ошибка канала RabbitMQ: %s", exc)
            except KeyboardInterrupt:
                logger.info("Прервано пользователем")
                break
            except Exception as exc:
                logger.error("Неожиданная ошибка: %s", exc)

            # Если сервис остановлен — не переподключаемся
            if self._stop_event.is_set():
                break

            attempt += 1
            if (
                config.MAX_RECONNECT_ATTEMPTS > 0
                and attempt > config.MAX_RECONNECT_ATTEMPTS
            ):
                logger.error(
                    "Превышено максимальное число попыток переподключения (%s).",
                    config.MAX_RECONNECT_ATTEMPTS,
                )
                break

            # Закрываем старое соединение перед переподключением
            self._close_rabbitmq()

            logger.info(
                "Переподключение через %s сек... (попытка %s)",
                config.RECONNECT_DELAY,
                attempt,
            )
            # Ждём с возможностью прерывания через stop()
            if self._stop_event.wait(config.RECONNECT_DELAY):
                break

        self.stop()

    def _close_rabbitmq(self):
        try:
            if self.rabbit_channel and self.rabbit_channel.is_open:
                self.rabbit_channel.close()
        except Exception:
            pass
        try:
            if self.rabbit_connection and self.rabbit_connection.is_open:
                self.rabbit_connection.close()
        except Exception:
            pass
        self.rabbit_channel = None
        self.rabbit_connection = None

    def stop(self):
        logger.info("Остановка сервиса аналитики...")
        self._stop_event.set()

        self._close_rabbitmq()

        if self.redis_client:
            self.redis_client.close()

        if self._stats_thread and self._stats_thread.is_alive():
            self._stats_thread.join(timeout=config.STATS_INTERVAL + 1)

        logger.info("Сервис аналитики остановлен")


if __name__ == "__main__":
    service = AnalyticsService()
    service.start()
