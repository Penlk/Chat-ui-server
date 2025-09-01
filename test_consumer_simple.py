"""
Простой тест Kafka consumer без Django
"""
import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_GROUP_ID = "chat-service-test-consumers"
KAFKA_TOPIC = "chat-ui-messages"

async def process_stream_message(message_data):
    """Обработка сообщения для запуска stream (упрощенная версия)"""
    try:
        # Проверяем на ошибки JSON
        if isinstance(message_data, dict) and message_data.get("error") == "json_decode_failed":
            logger.error(f"💥 Пропускаем сообщение с ошибкой JSON: {message_data.get('raw_message', 'Unknown')}")
            return
        
        # Извлекаем данные из сообщения
        operation = message_data.get("operation")
        payload = message_data.get("payload", {})
        request_id = message_data.get("request_id")
        message_id = message_data.get("message_id")
        timestamp = message_data.get("timestamp")
        
        logger.info(f"📨 Processing message: {message_id}")
        logger.info(f"🔧 Operation: {operation}")
        logger.info(f"🆔 Request ID: {request_id}")
        logger.info(f"⏰ Timestamp: {timestamp}")
        
        if operation == "start_stream":
            # Извлекаем данные для streaming
            saved_message_id = payload.get("saved_message_id")
            conversation_id = payload.get("conversation_id")
            message_text = payload.get("message")
            user_context = payload.get("user_context", {})
            user_email = user_context.get("email")
            
            logger.info(f"💬 Message ID: {saved_message_id}")
            logger.info(f"🗨️  Conversation ID: {conversation_id}")
            logger.info(f"📝 Message: {message_text[:50]}...")
            logger.info(f"👤 User: {user_email}")
            
            # Здесь будет вызов api/conversation_cloud для streaming
            logger.info(f"🤖 Starting AI processing for stream_id: {request_id}")
            logger.info(f"✅ AI processing completed for stream_id: {request_id}")
            
        else:
            logger.warning(f"❓ Unknown operation: {operation}")
            
    except Exception as e:
        logger.error(f"💥 Error processing message: {e}")

async def test_consumer():
    """Тест consumer с обработкой сообщений"""
    try:
        logger.info("🧪 Создаем Kafka consumer...")
        
        def safe_json_deserializer(message):
            """Безопасный десериализатор JSON с обработкой ошибок"""
            try:
                decoded = message.decode('utf-8')
                logger.info(f"📄 Raw message: {decoded[:100]}...")
                return json.loads(decoded)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"❌ JSON decode error: {e}")
                logger.error(f"📄 Raw bytes: {message}")
                return {"error": "json_decode_failed", "raw_message": str(message)}
        
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"{KAFKA_GROUP_ID}-new",  # Новая группа для получения всех сообщений
            value_deserializer=safe_json_deserializer,
            auto_offset_reset='earliest',  # Читаем все сообщения с начала
            enable_auto_commit=True,
            max_poll_records=50  # Больше сообщений для анализа
        )
        
        await consumer.start()
        logger.info(f"✅ Kafka consumer started successfully!")
        logger.info(f"📡 Listening to topic: {KAFKA_TOPIC}")
        logger.info(f"👥 Consumer group: {KAFKA_GROUP_ID}")
        logger.info("📡 Ждем новые сообщения (30 секунд)...")
        
        message_count = 0
        timeout = 30  # 30 секунд
        
        async def consume_with_timeout():
            nonlocal message_count
            async for message in consumer:
                message_count += 1
                logger.info(f"📬 Received message #{message_count}")
                logger.info(f"   Topic: {message.topic}")
                logger.info(f"   Partition: {message.partition}")
                logger.info(f"   Offset: {message.offset}")
                
                # Обработка сообщения
                await process_stream_message(message.value)
                
                # Подтверждение обработки
                await consumer.commit()
                logger.info(f"✅ Message #{message_count} processed successfully")
                
                # Останавливаемся после 10 сообщений для анализа
                if message_count >= 10:
                    logger.info("🛑 Получено 10 сообщений, завершаем тест")
                    break
        
        # Запускаем с таймаутом
        try:
            await asyncio.wait_for(consume_with_timeout(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.info(f"⏰ Timeout reached ({timeout} seconds)")
        
        logger.info(f"✅ Тест завершен! Обработано сообщений: {message_count}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в consumer: {e}")
        
    finally:
        await consumer.stop()
        logger.info("🛑 Consumer stopped")

if __name__ == "__main__":
    asyncio.run(test_consumer())
