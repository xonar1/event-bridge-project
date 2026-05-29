Сервис-consumer, который слушает очередь `excel_report_queue` в RabbitMQ и ведёт оперативный отчёт для менеджеров в Excel-файле `events_report.xlsx`.

## 🧩 Технологии
- **Pika** — AMQP-клиент для RabbitMQ
- **OpenPyXL** — чтение/запись `.xlsx`
- **Pandas** — опционально для аналитики

## 🔧 Ключевые особенности
- При старте проверяет наличие `events_report.xlsx`. Если файла нет — создаёт его с заголовками.
- **Буферизация:** входящие регистрации накапливаются в памяти (до 10 штук) и пишутся в файл пачкой — защита от частых открытий/записей файла.
- Страховочный сброс буфера по таймеру (каждые 30 сек).
- `basic_ack` вызывается сразу **после добавления записи в буфер**.
- Корректное завершение по `Ctrl+C` с финальным flush'ем буфера.

## 🚀 Запуск

```bash
# Из корня проекта:
python excel_report_service/main.py

# Тестовый producer (15 сообщений с интервалом 0.5 сек):
python excel_report_service/test_producer.py 15 0.5
```
# docker compose up --build -d
# docker logs eventbridge_excel_report
# cd "C:\Users\Академия\MyDocuments\SavedGames\Новая папка (3)\event-bridge-project\excel_report_service"
## 📦 Зависимости
```bash
pip install -r excel_report_service/requirements.txt
```
