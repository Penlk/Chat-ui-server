#!/usr/bin/env python3
"""
Простой тест Consumer для проверки обработки сообщений без Cloud.ru API
"""
import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_consumer():
    """Простой тест Consumer"""
    try:
        # Создаем consumer
        consumer = AIOKafkaConsumer(
            'chat-ui-messages',
            bootstrap_servers='chat-service-kafka:29092',
            group_id='test-consumer-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        await consumer.start()
        logger.info("✅ Consumer запущен")
        
        # Читаем сообщения
        async for msg in consumer:
            logger.info(f"📨 Получено сообщение: {msg.value}")
            
            # Простая обработка
            data = msg.value
            if data.get('operation') == 'start_stream':
                logger.info(f"🚀 Обрабатываем start_stream для сообщения {data.get('request_id')}")
                
                # Здесь должна быть логика вызова Cloud.ru API
                logger.info("✅ Обработка завершена (заглушка)")
            
            # Обрабатываем только одно сообщение для теста
            break
        
        await consumer.stop()
        logger.info("✅ Consumer остановлен")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в Consumer: {e}")

def main():
    """Основная функция"""
    logger.info("🚀 Запускаем тест Consumer...")
    asyncio.run(test_consumer())

if __name__ == "__main__":
    main()
