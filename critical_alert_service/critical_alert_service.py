# critical_alert_service.py
import pika
import json
import os
import sys
import threading
from typing import Dict, Any


# Конфигурация из переменных окружения с значениями по умолчанию
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
QUEUE_NAME = os.getenv('QUEUE_NAME', 'critical_alert_queue')

# Флаг для запуска тестов API Gateway
RUN_API_TESTS = os.getenv('RUN_API_TESTS', 'true').lower() == 'true'


def get_rabbitmq_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    return pika.BlockingConnection(parameters)


def print_vip_alert(event_data: Dict[str, Any]):
    user_name = event_data.get('user_name', 'Unknown')
    user_email = event_data.get('user_email', 'unknown@example.com')
    event_name = event_data.get('event_name', 'Unknown Event')
    
    lines = [
        "!!! NEW VIP REGISTRATION !!!",
        f"User: {user_email}",
        f"Event: {event_name}"
    ]
    
    max_length = max(len(line) for line in lines) + 4
    
    print("\n" + "*" * max_length)
    for line in lines:
        padding = max_length - len(line) - 2
        print(f"* {line}{' ' * padding}*")
    print("*" * max_length + "\n")


def callback(ch, method, properties, body):
    try:
        event_data = json.loads(body.decode('utf-8'))
        
        if event_data.get('is_vip'):
            print_vip_alert(event_data)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            print(f"[✓] Обработано VIP-событие: {event_data.get('user_email')}")
        else:
            print(f"[!] Получено non-VIP событие в очереди {QUEUE_NAME}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
    except json.JSONDecodeError as e:
        print(f"[ERROR] Ошибка парсинга JSON: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as e:
        print(f"[ERROR] Ошибка обработки: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def run_api_tests():
    """Запускает интеграционные тесты API Gateway в отдельном потоке"""
    try:
        from test_api_gateway import send_test_requests_to_api_gateway
        print("\n🔧 Запуск интеграционных тестов API Gateway...")
        success = send_test_requests_to_api_gateway()
        if success:
            print("✅ API Gateway доступен и работает корректно")
        else:
            print("⚠️ API Gateway тесты не прошли, но сервис продолжит работу")
    except ImportError:
        print("⚠️ Модуль test_api_gateway не найден, пропускаем тесты")
    except Exception as e:
        print(f"⚠️ Ошибка при запуске тестов API Gateway: {e}")


def main():
    print("=" * 50)
    print("VIP Консьерж Сервис (Critical Alert Service)")
    print("=" * 50)
    print(f"RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    print(f"Ожидаемая очередь: {QUEUE_NAME}")
    print("-" * 50)
    
    # Запускаем тесты API Gateway, если включено
    if RUN_API_TESTS:
        import time
        time.sleep(2)
        api_test_thread = threading.Thread(target=run_api_tests, daemon=True)
        api_test_thread.start()
    
    print("Ожидание VIP-регистраций...")
    print("Нажмите CTRL+C для выхода")
    print("-" * 50)
    
    while True:
        try:
            connection = get_rabbitmq_connection()
            channel = connection.channel()
            
            # НЕ создаём exchange и очередь — они уже созданы оркестратором
            # Только подписываемся на существующую очередь
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=callback,
                auto_ack=False
            )
            print(f"[✓] Начат приём сообщений из очереди '{QUEUE_NAME}'")
            channel.start_consuming()
            
        except pika.exceptions.AMQPConnectionError as e:
            print(f"[!] Ошибка подключения к RabbitMQ: {e}")
            print("[!] Повтор через 5 секунд...")
            import time
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n[✓] Завершение работы...")
            try:
                connection.close()
            except:
                pass
            sys.exit(0)
        except Exception as e:
            print(f"[!] Ошибка: {e}")
            print("[!] Перезапуск через 5 секунд...")
            import time
            time.sleep(5)


if __name__ == '__main__':
    main()