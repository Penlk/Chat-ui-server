"""
Улучшенный Kafka consumer для сбора фрагментов JSON в полные сообщения
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
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_GROUP_ID = "chat-service-improved-consumers"
KAFKA_TOPIC = "chat-ui-messages"

class MessageCollector:
    """Собирает фрагменты JSON в полные сообщения"""
    
    def __init__(self):
        self.fragments = []
        self.brace_count = 0
        self.current_message = ""
        
    def add_fragment(self, fragment):
        """Добавляет фрагмент и пытается собрать полное сообщение"""
        self.current_message += fragment
        
        # Подсчитываем скобки
        for char in fragment:
            if char == '{':
                self.brace_count += 1
            elif char == '}':
                self.brace_count -= 1
                
        # Если скобки сбалансированы, пытаемся парсить JSON
        if self.brace_count == 0 and self.current_message.strip():
            try:
                message_data = json.loads(self.current_message.strip())
                logger.info(f"✅ Собрано полное сообщение: {message_data.get('message_id', 'unknown')}")
                self.current_message = ""
                return message_data
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ Не удалось распарсить JSON: {e}")
                self.current_message = ""
                return None
                
        return None

async def process_stream_message(message_data):
    """Обработка сообщения для запуска stream (упрощенная версия)"""
    try:
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
            
            # TODO: Здесь будет вызов api/conversation_cloud для streaming
            logger.info(f"🤖 Starting AI processing for stream_id: {request_id}")
            logger.info(f"   - Message ID: {saved_message_id}")
            logger.info(f"   - Conversation ID: {conversation_id}")
            logger.info(f"   - User: {user_email}")
            logger.info(f"📤 User message: {message_text}")
            logger.info(f"✅ AI processing completed for stream_id: {request_id}")
            
        else:
            logger.warning(f"❓ Unknown operation: {operation}")
            
    except Exception as e:
        logger.error(f"💥 Error processing message: {e}")
        logger.error(f"📄 Message data: {message_data}")

async def main():
    """Основная функция"""
    try:
        logger.info("🧪 Создаем улучшенный Kafka consumer...")
        
        def safe_json_deserializer(message):
            try:
                decoded = message.decode('utf-8')
                return decoded
            except UnicodeDecodeError as e:
                logger.error(f"❌ Unicode decode error: {e}")
                return None
        
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"{KAFKA_GROUP_ID}-{int(time.time())}",
            value_deserializer=safe_json_deserializer,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            max_poll_records=100
        )
        
        await consumer.start()
        logger.info("✅ Kafka consumer started successfully!")
        logger.info(f"📡 Listening to topic: {KAFKA_TOPIC}")
        logger.info(f"👥 Consumer group: {KAFKA_GROUP_ID}")
        
        # Создаем коллектор сообщений
        collector = MessageCollector()
        
        logger.info("📡 Ждем новые сообщения (60 секунд)...")
        
        message_count = 0
        timeout = 60  # 60 секунд
        
        async def collect_messages():
            nonlocal message_count
            async for message in consumer:
                try:
                    message_count += 1
                    raw_value = message.value
                    
                    if raw_value is None:
                        continue
                        
                    logger.info(f"📬 Received message #{message_count}")
                    logger.info(f"   Topic: {message.topic}")
                    logger.info(f"   Partition: {message.partition}")
                    logger.info(f"   Offset: {message.offset}")
                    logger.info(f"📄 Raw fragment: {raw_value[:100]}...")
                    
                    # Добавляем фрагмент в коллектор
                    complete_message = collector.add_fragment(raw_value)
                    
                    if complete_message:
                        # Обрабатываем полное сообщение
                        await process_stream_message(complete_message)
                        logger.info(f"✅ Complete message #{message_count} processed successfully")
                    else:
                        logger.info(f"📦 Fragment #{message_count} added to collector")
                    
                    # Подтверждаем обработку
                    await consumer.commit()
                    
                    # Если набрали много сообщений, прекращаем
                    if message_count >= 50:
                        break
                        
                except Exception as e:
                    logger.error(f"💥 Error processing message #{message_count}: {e}")
                    continue
        
        try:
            await asyncio.wait_for(collect_messages(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.info(f"⏰ Timeout {timeout}s - завершаем тест")
        
        logger.info(f"✅ Тест завершен! Обработано фрагментов: {message_count}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка в consumer: {e}")
        
    finally:
        await consumer.stop()
        logger.info("🛑 Consumer stopped")

if __name__ == "__main__":
    asyncio.run(main())
