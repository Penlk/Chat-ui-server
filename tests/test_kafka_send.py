#!/usr/bin/env python3
"""
Тестовый скрипт для отправки сообщений в Kafka изнутри контейнера
"""
import json
import asyncio
import sys
import os

# Добавляем путь к Django
sys.path.append('/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'chatgpt_ui_server.settings_postgres')

import django
django.setup()

from aiokafka import AIOKafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def send_message_to_kafka():
    """Отправка сообщения в Kafka"""
    try:
        # Подключаемся к Kafka (внутри Docker сети)
        producer = AIOKafkaProducer(
            bootstrap_servers='chat-service-kafka:29092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        await producer.start()
        logger.info("✅ Подключились к Kafka")
        
        # Тестовое сообщение
        message_data = {
            "operation": "start_stream",
            "request_id": "test-456",
            "payload": {
                "saved_message_id": 999,
                "conversation_id": 8,
                "message": "Расскажи мне подробно о технологиях будущего, включая искусственный интеллект, квантовые вычисления, биотехнологии и нанотехнологии. Опиши как эти технологии могут изменить мир в ближайшие 20 лет.",
                "user_context": {
                    "email": "test@example.com",
                    "sub": "test-user-123"
                }
            }
        }
        
        # Отправляем сообщение
        await producer.send(
            topic='chat-ui-messages',
            value=message_data,
            key='test-456'
        )
        
        logger.info(f"✅ Сообщение отправлено в Kafka: {message_data}")
        
        await producer.stop()
        logger.info("✅ Соединение с Kafka закрыто")
        
    except Exception as e:
        logger.error(f"❌ Ошибка отправки в Kafka: {e}")

def main():
    """Основная функция"""
    logger.info("🚀 Отправляем тестовое сообщение в Kafka...")
    asyncio.run(send_message_to_kafka())

if __name__ == "__main__":
    main()
