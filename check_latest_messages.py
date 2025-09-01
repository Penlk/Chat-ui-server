"""
Проверка последних сообщений в Kafka с новой consumer группой
"""
import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
import time

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "chat-ui-messages"

async def check_latest_messages():
    """Проверка последних сообщений с новой группой"""
    
    # Используем уникальную группу с timestamp
    unique_group = f"latest-check-{int(time.time())}"
    
    try:
        logger.info(f"🔍 Проверяем последние сообщения с группой: {unique_group}")
        
        def safe_json_deserializer(message):
            try:
                decoded = message.decode('utf-8')
                return json.loads(decoded)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"❌ JSON decode error: {e}")
                return {"error": "json_decode_failed", "raw": decoded[:100]}
        
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=unique_group,
            value_deserializer=safe_json_deserializer,
            auto_offset_reset='earliest',  # Читаем все с начала
            enable_auto_commit=False,  # Не сохраняем офсеты
            max_poll_records=100
        )
        
        await consumer.start()
        logger.info("✅ Consumer запущен")
        
        # Собираем все сообщения
        all_messages = []
        timeout = 10  # 10 секунд
        
        logger.info("📡 Читаем все сообщения...")
        
        async def collect_messages():
            async for message in consumer:
                msg_data = {
                    "partition": message.partition,
                    "offset": message.offset,
                    "timestamp": message.timestamp,
                    "value": message.value
                }
                all_messages.append(msg_data)
                
                # Если набрали много сообщений, прекращаем
                if len(all_messages) >= 50:
                    break
        
        try:
            await asyncio.wait_for(collect_messages(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.info(f"⏰ Timeout {timeout}s - анализируем собранные сообщения")
        
        logger.info(f"📊 Собрано сообщений: {len(all_messages)}")
        
        # Сортируем по timestamp для анализа последних
        all_messages.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
        
        logger.info("🔍 Последние 5 сообщений:")
        for i, msg in enumerate(all_messages[:5]):
            logger.info(f"\n--- Сообщение #{i+1} ---")
            logger.info(f"Partition: {msg['partition']}, Offset: {msg['offset']}")
            logger.info(f"Timestamp: {msg['timestamp']}")
            
            if isinstance(msg['value'], dict):
                if msg['value'].get('error') == 'json_decode_failed':
                    logger.info(f"❌ Битое сообщение: {msg['value']['raw']}")
                else:
                    # Правильное сообщение
                    operation = msg['value'].get('operation', 'unknown')
                    message_id = msg['value'].get('message_id', 'unknown')
                    request_id = msg['value'].get('request_id', 'unknown')
                    
                    payload = msg['value'].get('payload', {})
                    conversation_id = payload.get('conversation_id', 'unknown')
                    user_message = payload.get('message', '')
                    user_ctx = payload.get('user_context', {})
                    user_email = user_ctx.get('email', 'unknown')
                    
                    logger.info(f"✅ ВАЛИДНОЕ СООБЩЕНИЕ:")
                    logger.info(f"   Operation: {operation}")
                    logger.info(f"   Message ID: {message_id}")
                    logger.info(f"   Request ID: {request_id}")
                    logger.info(f"   Conversation: {conversation_id}")
                    logger.info(f"   User: {user_email}")
                    logger.info(f"   Message: {user_message[:50]}...")
            else:
                logger.info(f"🤷 Неизвестный формат: {msg['value']}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        
    finally:
        await consumer.stop()
        logger.info("🛑 Consumer остановлен")

if __name__ == "__main__":
    asyncio.run(check_latest_messages())
