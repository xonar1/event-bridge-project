# test_api_gateway.py
import pika
import json
import time
import os
import sys
from typing import Dict, Any

# Настройки RabbitMQ
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'guest')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'guest')
EXCHANGE_NAME = 'event_topic_exchange'
ROUTING_KEY = 'event.registered.vip'

def get_rabbitmq_connection():
    """Подключение к RabbitMQ с диагностикой"""
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=30,
            connection_attempts=3,
            retry_delay=2
        )
        connection = pika.BlockingConnection(parameters)
        return connection
    except Exception as e:
        print(f"   🔍 Диагностика подключения к RabbitMQ: {type(e).__name__}: {e}")
        return None

def send_event_to_rabbitmq(event_data: Dict[str, Any]) -> tuple[bool, str]:
    """Отправляет событие напрямую в RabbitMQ, возвращает (успех, сообщение)"""
    connection = None
    try:
        connection = get_rabbitmq_connection()
        if not connection:
            return False, "Не удалось подключиться к RabbitMQ"
        
        channel = connection.channel()
        
        # Убеждаемся, что exchange существует
        channel.exchange_declare(
            exchange=EXCHANGE_NAME,
            exchange_type='topic',
            durable=True,
            passive=False  # создаём если нет
        )
        
        # Публикуем сообщение
        channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ROUTING_KEY,
            body=json.dumps(event_data),
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type='application/json'
            )
        )
        
        connection.close()
        return True, "Успешно отправлено"
        
    except pika.exceptions.AMQPConnectionError as e:
        return False, f"Ошибка подключения: {e}"
    except pika.exceptions.ChannelError as e:
        return False, f"Ошибка канала: {e}"
    except Exception as e:
        return False, f"Ошибка: {type(e).__name__}: {e}"
    finally:
        if connection and not connection.is_closed:
            connection.close()

def send_test_requests_to_api_gateway():
    """Отправляет 5 разных тестовых событий напрямую в RabbitMQ"""
    
    print("\n" + "="*60)
    print("🧪 ЗАПУСК ТЕСТОВ (отправка событий в RabbitMQ)")
    print("="*60)
    print(f"   RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    print(f"   Exchange: {EXCHANGE_NAME}")
    print(f"   Routing key: {ROUTING_KEY}")
    print("="*60)
    
    # Сначала проверим подключение к RabbitMQ
    print("\n🔌 Проверка подключения к RabbitMQ...")
    test_conn = get_rabbitmq_connection()
    if test_conn:
        print("   ✅ Подключение к RabbitMQ установлено!")
        test_conn.close()
    else:
        print("   ❌ НЕ УДАЛОСЬ ПОДКЛЮЧИТЬСЯ К RABBITMQ!")
        print(f"   Проверьте: доступен ли RabbitMQ на {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        print("   Возможно, оркестратор ещё не запущен или очередь не создана")
        return False
    
    test_scenarios = [
        {
            "name": "Тест 1: Обычная регистрация (не VIP)",
            "data": {
                "user_name": "John Doe",
                "user_email": "john@example.com",
                "event_name": "Tech Conference 2024",
                "is_vip": False
            },
            "expect_vip_alert": False
        },
        {
            "name": "Тест 2: VIP регистрация #1",
            "data": {
                "user_name": "Sarah Connor",
                "user_email": "sarah.connor@example.com",
                "event_name": "AI Summit 2024",
                "is_vip": True
            },
            "expect_vip_alert": True
        },
        {
            "name": "Тест 3: VIP регистрация #2",
            "data": {
                "user_name": "Leonid Ivanov",
                "user_email": "leonid@example.com",
                "event_name": "Blockchain Conference",
                "is_vip": True
            },
            "expect_vip_alert": True
        },
        {
            "name": "Тест 4: VIP регистрация #3",
            "data": {
                "user_name": "Elena Petrova",
                "user_email": "elena@example.com",
                "event_name": "Data Science Forum",
                "is_vip": True
            },
            "expect_vip_alert": True
        },
        {
            "name": "Тест 5: VIP регистрация #4",
            "data": {
                "user_name": "Michael Wong",
                "user_email": "michael@example.com",
                "event_name": "Cloud Native Summit",
                "is_vip": True
            },
            "expect_vip_alert": True
        }
    ]
    
    results = []
    vip_sent = 0
    
    print("\n📨 Отправка событий в RabbitMQ...\n")
    
    for test in test_scenarios:
        print(f"📤 {test['name']}")
        print(f"   → VIP: {test['data']['is_vip']}")
        
        success, message = send_event_to_rabbitmq(test['data'])
        
        if success:
            if test['data']['is_vip']:
                vip_sent += 1
                print(f"   ✅ VIP-событие отправлено!")
            else:
                print(f"   ✅ Обычное событие отправлено")
            results.append({
                "test": test['name'],
                "status": "PASS",
                "is_vip": test['data']['is_vip']
            })
        else:
            print(f"   ❌ Ошибка: {message}")
            results.append({
                "test": test['name'],
                "status": "FAIL",
                "is_vip": test['data']['is_vip'],
                "error": message
            })
        
        time.sleep(0.3)  # Небольшая пауза
    
    print("\n" + "="*60)
    print("📊 ИТОГИ ТЕСТИРОВАНИЯ")
    print("="*60)
    
    passed = sum(1 for r in results if r['status'] == 'PASS')
    failed = len(results) - passed
    
    for r in results:
        status_icon = "✅" if r['status'] == 'PASS' else "❌"
        vip_mark = " [VIP]" if r['is_vip'] else ""
        print(f"{status_icon} {r['test']}: {r['status']}{vip_mark}")
    
    print(f"\n📈 Статистика:")
    print(f"   ✅ Успешно отправлено: {passed}")
    print(f"   ❌ Ошибок: {failed}")
    print(f"   🎉 VIP-событий отправлено: {vip_sent}")
    
    if passed == len(test_scenarios):
        print("\n🎉 ВСЕ ТЕСТЫ УСПЕШНО ВЫПОЛНЕНЫ!")
        return True
    else:
        print("\n⚠️ Тесты не прошли. Проверьте:")
        print("   1. Запущен ли RabbitMQ: docker ps | grep rabbitmq")
        print("   2. Имя хоста: должно быть 'rabbitmq' (не localhost)")
        print(f"   3. Порт: {RABBITMQ_PORT} должен быть доступен")
        return False


if __name__ == "__main__":
    print("⏳ Ожидание запуска RabbitMQ (5 секунд)...")
    time.sleep(5)
    send_test_requests_to_api_gateway()