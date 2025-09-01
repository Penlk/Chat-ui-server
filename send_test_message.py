"""
Отправка тестового сообщения в Kafka через aiokafka
"""
import asyncio
import json
import logging
from aiokafka import AIOKafkaProducer

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "chat-ui-messages"

async def send_test_message():
    """Отправка тестового сообщения"""
    try:
        logger.info("🚀 Создаем Kafka producer...")
        
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        await producer.start()
        logger.info("✅ Producer запущен!")
        
        # Тестовое сообщение
        test_message = {
            "message_id": "python-test-message-12345",
            "request_id": "python-test-request-12345",
            "timestamp": "2025-09-01T11:30:00.000Z",
            "payload": {
                "conversation_id": 21,
                "message": "🐍 Тестовое сообщение отправленное через Python aiokafka",
                "is_bot": False,
                "message_type": 0,
                "saved_message_id": 2000,
                "message_id": 3,
                "user_context": {
                    "email": "python@test.com",
                    "full_name": "Python Test User",
                    "active_org_id": None,
                    "org_role": "developer",
                    "is_org_owner": True
                },
                "request_metadata": {
                    "source_ip": "127.0.0.1",
                    "user_agent": "Python-aiokafka-Client"
                }
            },
            "operation": "start_stream"
        }
        
        logger.info(f"📤 Отправляем сообщение в топик: {KAFKA_TOPIC}")
        logger.info(f"🆔 Message ID: {test_message['message_id']}")
        logger.info(f"🔧 Operation: {test_message['operation']}")
        
        # Отправляем сообщение
        await producer.send(KAFKA_TOPIC, test_message)
        
        # Ждем подтверждения отправки
        await producer.flush()
        
        logger.info("✅ Сообщение отправлено успешно!")
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки: {e}")
        
    finally:
        await producer.stop()
        logger.info("🛑 Producer остановлен")

if __name__ == "__main__":
    asyncio.run(send_test_message())
