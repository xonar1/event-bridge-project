import logging

from config import settings

logging.basicConfig(
    level=settings.log_level,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

from controller.run import run_all  # noqa: E402

if __name__ == "__main__":
    run_all()
