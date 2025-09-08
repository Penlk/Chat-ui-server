"""
Тестовый скрипт для проверки подключения к Kafka
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

async def test_kafka_connection():
    """Тест подключения к Kafka"""
    consumer = ChatKafkaConsumer()
    
    try:
        logger.info("🧪 Testing Kafka connection...")
        
        # Пытаемся подключиться
        await consumer.start_consumer()
        
        logger.info("✅ Kafka connection successful!")
        logger.info("📡 Waiting for messages for 30 seconds...")
        
        # Ждем сообщения 30 секунд
        await asyncio.wait_for(
            consumer.consume_messages(),
            timeout=30.0
        )
        
    except asyncio.TimeoutError:
        logger.info("⏰ Timeout reached (30 seconds)")
        logger.info("✅ Connection test completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Kafka connection failed: {e}")
        
    finally:
        await consumer.stop_consumer()

if __name__ == "__main__":
    asyncio.run(test_kafka_connection())
