#!/usr/bin/env python3
"""
Простой скрипт для отправки сообщений в Kafka
"""
import json
import asyncio
from aiokafka import AIOKafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def send_message_to_kafka(message_data):
    """Отправка сообщения в Kafka"""
    try:
        # Подключаемся к Kafka
        producer = AIOKafkaProducer(
            bootstrap_servers='localhost:9095',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        await producer.start()
        logger.info("✅ Подключились к Kafka")
        
        # Отправляем сообщение
        await producer.send(
            topic='chat-ui-messages',
            value=message_data,
            key=str(message_data.get('request_id', 'test'))
        )
        
        logger.info(f"✅ Сообщение отправлено в Kafka: {message_data}")
        
        await producer.stop()
        logger.info("✅ Соединение с Kafka закрыто")
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки в Kafka: {e}")

def main():
    """Основная функция"""
    # Тестовое сообщение
    message_data = {
        "operation": "start_stream",
        "request_id": "test-123",
        "payload": {
            "saved_message_id": 999,
            "conversation_id": 8,
            "message": "Тестовое сообщение для проверки Kafka",
            "user_context": {
                "email": "test@example.com",
                "sub": "test-user-123"
            }
        }
    }
    
    logger.info("🚀 Отправляем тестовое сообщение в Kafka...")
    asyncio.run(send_message_to_kafka(message_data))

if __name__ == "__main__":
    main()
