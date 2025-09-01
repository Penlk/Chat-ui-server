"""
Проверка сырых сообщений в Kafka без JSON десериализации
"""
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "chat-ui-messages"

async def check_raw_messages():
    """Проверка сырых сообщений без JSON парсинга"""
    
    # Используем уникальную группу с timestamp
    unique_group = f"raw-check-{int(time.time())}"
    
    try:
        logger.info(f"🔍 Проверяем сырые сообщения с группой: {unique_group}")
        
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=unique_group,
            value_deserializer=lambda m: m.decode('utf-8'),  # Просто декодируем в строку
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=100
        )
        
        await consumer.start()
        logger.info("✅ Consumer запущен")
        
        # Собираем все сообщения
        all_messages = []
        timeout = 10
        
        logger.info("📡 Читаем сырые сообщения...")
        
        async def collect_messages():
            async for message in consumer:
                msg_data = {
                    "partition": message.partition,
                    "offset": message.offset,
                    "timestamp": message.timestamp,
                    "raw_value": message.value
                }
                all_messages.append(msg_data)
                
                if len(all_messages) >= 50:
                    break
        
        try:
            await asyncio.wait_for(collect_messages(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.info(f"⏰ Timeout {timeout}s")
        
        logger.info(f"📊 Собрано сообщений: {len(all_messages)}")
        
        # Сортируем по timestamp
        all_messages.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
        
        logger.info("🔍 Последние 10 сообщений (сырые):")
        for i, msg in enumerate(all_messages[:10]):
            logger.info(f"\n--- Сообщение #{i+1} ---")
            logger.info(f"Partition: {msg['partition']}, Offset: {msg['offset']}")
            logger.info(f"Timestamp: {msg['timestamp']}")
            logger.info(f"Длина: {len(msg['raw_value'])} символов")
            
            raw = msg['raw_value']
            logger.info(f"Содержимое: {raw[:100]}...")
            
            # Проверяем, выглядит ли как полный JSON
            if raw.startswith('{') and raw.endswith('}') and 'operation' in raw:
                logger.info("✅ ПОХОЖЕ НА ПОЛНЫЙ JSON!")
                
                # Попробуем найти ключевые поля
                if 'start_stream' in raw:
                    logger.info("🎯 Содержит operation: start_stream")
                if 'message_id' in raw:
                    logger.info("🆔 Содержит message_id")
                if 'conversation_id' in raw:
                    logger.info("💬 Содержит conversation_id")
                if 'stream будет работать' in raw:
                    logger.info("📩 ЭТО ВАШЕ НОВОЕ СООБЩЕНИЕ!")
            else:
                logger.info("❌ Битый фрагмент JSON")
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        
    finally:
        await consumer.stop()
        logger.info("🛑 Consumer остановлен")

if __name__ == "__main__":
    asyncio.run(check_raw_messages())
