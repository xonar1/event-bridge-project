FROM python:3.11-slim

WORKDIR /app

# Устанавливаем зависимости
RUN pip install --no-cache-dir pika requests python-dotenv

# Копируем файлы
COPY critical_alert_service.py .
COPY test_api_gateway.py .
COPY .env .

# Запускаем сервис
CMD ["python", "-u", "critical_alert_service.py"]