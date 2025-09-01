"""
Тест полного Kafka consumer с таймаутом
"""
import asyncio
import sys
import os

# Добавляем путь к Django проекту
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Настройка Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'chatgpt_ui_server.settings')
import django
django.setup()

from chat.kafka_consumer import ChatKafkaConsumer
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_full_consumer():
    """Тест полного consumer с ограничением по времени"""
    consumer = ChatKafkaConsumer()
    
    try:
        logger.info("🧪 Тестируем полный Kafka consumer...")
        
        # Запускаем consumer
        await consumer.start_consumer()
        
        logger.info("✅ Consumer запущен успешно!")
        logger.info("📡 Ждем сообщения 30 секунд...")
        
        # Ждем сообщения максимум 30 секунд
        message_count = 0
        async for message in consumer.consumer:
            message_count += 1
            logger.info(f"📨 Получено сообщение #{message_count}")
            
            # Обработка сообщения
            await consumer.process_stream_message(message.value)
            
            # Подтверждение обработки
            await consumer.consumer.commit()
            
            # Останавливаемся после 3 сообщений или по таймауту
            if message_count >= 3:
                logger.info("🛑 Получено 3 сообщения, завершаем тест")
                break
        
        logger.info(f"✅ Тест завершен! Обработано сообщений: {message_count}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в consumer: {e}")
        
    finally:
        await consumer.stop_consumer()

if __name__ == "__main__":
    asyncio.run(test_full_consumer())
