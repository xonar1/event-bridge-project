import logging
import threading

from config import settings
from wiev.pdfSrver import app
from controller.consumer import run_consumer

logger = logging.getLogger(settings.service_name)


def start_server() -> None:
    logger.info("Starting HTTP server on %s:%s", settings.server_host, settings.server_port)
    app.run(host=settings.server_host, port=settings.server_port)


def run_all() -> None:
    threading.Thread(target=start_server, daemon=True).start()
    threading.Thread(target=run_consumer, daemon=True).start()
    logger.info("Server running on http://%s:%s", settings.server_host, settings.server_port)
    logger.info("RabbitMQ consumer listening for registration events …")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        logger.info("Shutting down.")
