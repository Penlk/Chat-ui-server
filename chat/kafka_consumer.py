"""
Kafka Consumer для Chat Service
Подписывается на топик chat-ui-messages для получения сообщений и запуска streaming
"""
import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from django.conf import settings
from asgiref.sync import sync_to_async

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = getattr(settings, 'KAFKA_BOOTSTRAP_SERVERS', "kafka:9092")
KAFKA_GROUP_ID = "chat-service-consumers"
KAFKA_TOPIC = "chat-ui-messages"

class ChatKafkaConsumer:
    """Kafka Consumer для обработки сообщений чата"""
    
    def __init__(self):
        self.consumer = None
        self.running = False
        
    async def start_consumer(self):
        """Инициализация и запуск consumer"""
        try:
            logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            
            self.consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=10  # Ограничиваем количество сообщений за раз
            )
            
            await self.consumer.start()
            logger.info(f"✅ Kafka consumer started successfully!")
            logger.info(f"📡 Listening to topic: {KAFKA_TOPIC}")
            logger.info(f"👥 Consumer group: {KAFKA_GROUP_ID}")
            
            self.running = True
            
        except Exception as e:
            logger.error(f"❌ Failed to start Kafka consumer: {e}")
            raise
    
    async def stop_consumer(self):
        """Остановка consumer"""
        if self.consumer:
            self.running = False
            await self.consumer.stop()
            logger.info("🛑 Kafka consumer stopped")
    
    async def process_stream_message(self, message_data):
        """Обработка сообщения для запуска stream"""
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
                await self.start_ai_processing(
                    message_id=saved_message_id,
                    conversation_id=conversation_id,
                    user_message=message_text,
                    user_email=user_email,
                    stream_id=request_id
                )
                
            else:
                logger.warning(f"❓ Unknown operation: {operation}")
                
        except Exception as e:
            logger.error(f"💥 Error processing message: {e}")
            logger.error(f"📄 Message data: {message_data}")
    
    async def start_ai_processing(self, message_id, conversation_id, user_message, user_email, stream_id):
        """Запуск AI обработки (заглушка для тестирования)"""
        try:
            logger.info(f"🤖 Starting AI processing for stream_id: {stream_id}")
            logger.info(f"   - Message ID: {message_id}")
            logger.info(f"   - Conversation ID: {conversation_id}")
            logger.info(f"   - User: {user_email}")
            
            # Временная заглушка - просто логируем что получили
            logger.info(f"📤 User message: {user_message}")
            
            # TODO: Здесь будет:
            # 1. Получение истории разговора из БД
            # 2. Вызов api/conversation_cloud с streaming
            # 3. Передача stream на фронтенд
            # 4. Сохранение ответа AI в БД
            
            logger.info(f"✅ AI processing completed for stream_id: {stream_id}")
            
        except Exception as e:
            logger.error(f"💥 AI processing failed for stream_id: {stream_id}, error: {e}")
    
    async def consume_messages(self):
        """Основной цикл потребления сообщений из Kafka"""
        logger.info("🚀 Starting message consumption loop...")
        
        try:
            message_count = 0
            async for message in self.consumer:
                try:
                    message_count += 1
                    logger.info(f"📬 Received message #{message_count}")
                    logger.info(f"   Topic: {message.topic}")
                    logger.info(f"   Partition: {message.partition}")
                    logger.info(f"   Offset: {message.offset}")
                    
                    # Обработка сообщения
                    await self.process_stream_message(message.value)
                    
                    # Подтверждение обработки
                    await self.consumer.commit()
                    logger.info(f"✅ Message #{message_count} processed successfully")
                    
                except Exception as e:
                    logger.error(f"💥 Error processing message #{message_count}: {e}")
                    # Продолжаем обработку следующих сообщений
                    
                # Проверяем флаг остановки
                if not self.running:
                    logger.info("🛑 Stopping consumer loop...")
                    break
                    
        except Exception as e:
            logger.error(f"💥 Consumer loop error: {e}")
        finally:
            await self.stop_consumer()

# Глобальный экземпляр consumer
kafka_consumer = ChatKafkaConsumer()

async def start_kafka_consumer():
    """Функция для запуска Kafka consumer"""
    try:
        await kafka_consumer.start_consumer()
        await kafka_consumer.consume_messages()
    except Exception as e:
        logger.error(f"💥 Failed to start Kafka consumer: {e}")

async def stop_kafka_consumer():
    """Функция для остановки Kafka consumer"""
    await kafka_consumer.stop_consumer()

# Для тестирования
if __name__ == "__main__":
    asyncio.run(start_kafka_consumer())
