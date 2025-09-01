"""
Упрощенный Kafka Consumer Service для Chat Service
Запускается как отдельный процесс без Django зависимостей
"""
import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, Any

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', "kafka:29092")
KAFKA_GROUP_ID = "chat-service-consumers"
KAFKA_TOPIC = "chat-ui-messages"

class MessageCollector:
    """Собирает фрагменты JSON в полные сообщения"""
    
    def __init__(self):
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

class ChatKafkaConsumerService:
    """Сервис для обработки сообщений из Kafka"""
    
    def __init__(self):
        self.consumer = None
        self.running = False
        
    async def start(self):
        """Запуск Kafka consumer"""
        try:
            logger.info(f"🚀 Starting Chat Kafka Consumer Service...")
            logger.info(f"📡 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            logger.info(f"🎯 Topic: {KAFKA_TOPIC}")
            logger.info(f"👥 Consumer group: {KAFKA_GROUP_ID}")
            
            # Импортируем aiokafka здесь, чтобы избежать проблем с зависимостями
            from aiokafka import AIOKafkaConsumer
            
            self.consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                value_deserializer=lambda m: m.decode('utf-8'),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                max_poll_records=10
            )
            
            await self.consumer.start()
            logger.info("✅ Kafka consumer started successfully!")
            
            self.running = True
            await self._consume_messages()
            
        except Exception as e:
            logger.error(f"❌ Failed to start Kafka consumer: {e}")
            raise
    
    async def stop(self):
        """Остановка consumer"""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info("🛑 Kafka consumer stopped")
    
    async def _consume_messages(self):
        """Основной цикл потребления сообщений"""
        try:
            # Создаем коллектор для фрагментов
            collector = MessageCollector()
            
            async for message in self.consumer:
                if not self.running:
                    break
                    
                try:
                    raw_value = message.value
                    if raw_value is None:
                        continue
                        
                    logger.info(f"📬 Received fragment: {raw_value[:50]}...")
                    
                    # Добавляем фрагмент в коллектор
                    complete_message = collector.add_fragment(raw_value)
                    
                    if complete_message:
                        # Обрабатываем полное сообщение
                        await self._process_message_data(complete_message)
                        logger.info(f"✅ Complete message processed")
                    else:
                        logger.info(f"📦 Fragment added to collector")
                        
                except Exception as e:
                    logger.error(f"💥 Error processing message: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Consumer error: {e}")
        finally:
            await self.stop()
    
    async def _process_message_data(self, data):
        """Обработка полного сообщения"""
        try:
            logger.info(f"📬 Processing message: {data.get('message_id', 'unknown')}")
            
            operation = data.get('operation')
            if operation == 'start_stream':
                await self._handle_start_stream(data)
            else:
                logger.warning(f"❓ Unknown operation: {operation}")
                
        except Exception as e:
            logger.error(f"💥 Error in _process_message_data: {e}")
    
    async def _handle_start_stream(self, data: Dict[str, Any]):
        """Обработка операции start_stream"""
        try:
            payload = data.get('payload', {})
            request_id = data.get('request_id')
            
            # Извлекаем данные
            saved_message_id = payload.get('saved_message_id')
            conversation_id = payload.get('conversation_id')
            message_text = payload.get('message')
            user_context = payload.get('user_context', {})
            user_email = user_context.get('email')
            
            logger.info(f"🤖 Starting AI processing for message {saved_message_id}")
            logger.info(f"   - Conversation ID: {conversation_id}")
            logger.info(f"   - User: {user_email}")
            logger.info(f"   - Message: {message_text[:50]}...")
            
            # Вызываем api/conversation_cloud для streaming
            await self._call_conversation_cloud(
                message_id=saved_message_id,
                conversation_id=conversation_id,
                message=message_text,
                user_context=user_context,
                request_id=request_id
            )
            
        except Exception as e:
            logger.error(f"💥 Error in _handle_start_stream: {e}")
    
    async def _call_conversation_cloud(self, message_id, conversation_id, message, user_context, request_id):
        """Вызов api/conversation_cloud для streaming"""
        try:
            # Формируем запрос к api/conversation_cloud
            conversation_data = {
                'conversation_id': conversation_id,
                'message': message,
                'is_bot': False,
                'message_type': 0,
                'user_context': user_context,
                'request_id': request_id
            }
            
            logger.info(f"📡 Calling api/conversation_cloud for message {message_id}")
            
            # Здесь будет вызов api/conversation_cloud
            # Пока что просто логируем
            logger.info(f"✅ AI processing completed for message {message_id}")
            logger.info(f"   - Request ID: {request_id}")
            logger.info(f"   - Conversation ID: {conversation_id}")
            logger.info(f"   - User: {user_context.get('email')}")
            
            # TODO: Реальный вызов api/conversation_cloud
            # response = await self._make_conversation_request(conversation_data)
            
        except Exception as e:
            logger.error(f"💥 Error calling conversation_cloud: {e}")
    
    async def _make_conversation_request(self, data):
        """Выполнение HTTP запроса к api/conversation_cloud"""
        # TODO: Реализовать HTTP запрос к api/conversation_cloud
        pass

async def main():
    """Основная функция"""
    service = ChatKafkaConsumerService()
    
    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("🛑 Received interrupt signal")
    except Exception as e:
        logger.error(f"❌ Service error: {e}")
    finally:
        await service.stop()

if __name__ == "__main__":
    asyncio.run(main())
