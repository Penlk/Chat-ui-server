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
        """Запуск AI обработки с streaming через cloud.ru API"""
        try:
            logger.info(f"🤖 Starting AI processing for stream_id: {stream_id}")
            logger.info(f"   - Message ID: {message_id}")
            logger.info(f"   - Conversation ID: {conversation_id}")
            logger.info(f"   - User: {user_email}")
            
            # Получаем API ключ из переменных окружения
            import os
            openai_api_key = os.getenv('OPENAI_API_KEY')
            if not openai_api_key:
                logger.error("❌ OPENAI_API_KEY not found in environment variables")
                return
            
            # Импортируем openai для streaming
            import openai
            
            # Настраиваем OpenAI для cloud.ru
            openai.api_key = openai_api_key
            openai.api_base = "https://foundation-models.api.cloud.ru/v1"
            
            logger.info(f"🌐 Using cloud.ru API: {openai.api_base}")
            
            # Формируем сообщения для API
            messages = [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": user_message}
            ]
            
            # Вызываем cloud.ru API с streaming
            logger.info(f"📡 Calling cloud.ru API with streaming...")
            
            response = openai.ChatCompletion.create(
                model="deepseek-ai/DeepSeek-R1-Distill-Llama-70B",
                messages=messages,
                max_tokens=1000,
                temperature=0.7,
                top_p=0.95,
                presence_penalty=0,
                frequency_penalty=0,
                stream=True
            )
            
            # Собираем полный ответ и отправляем на фронтенд
            full_response = ""
            chunk_count = 0
            
            for chunk in response:
                chunk_count += 1
                
                # Проверяем структуру chunk
                if hasattr(chunk, 'choices') and chunk.choices:
                    choice = chunk.choices[0]
                    
                    if hasattr(choice, 'delta') and choice.delta:
                        delta = choice.delta
                        
                        # Обрабатываем reasoning_content (DeepSeek модель)
                        if hasattr(delta, 'reasoning_content') and delta.reasoning_content:
                            content = delta.reasoning_content
                            full_response += content
                            
                            # Отправляем chunk на фронтенд
                            await self.send_chunk_to_frontend(content, stream_id)
                            
                        # Обрабатываем content (стандартный OpenAI)
                        elif hasattr(delta, 'content') and delta.content:
                            content = delta.content
                            full_response += content
                            
                            # Отправляем chunk на фронтенд
                            await self.send_chunk_to_frontend(content, stream_id)
                
                # Ограничиваем количество chunk'ов для тестирования
                if chunk_count >= 100:  # Увеличиваем лимит для полного ответа
                    logger.info(f"⏹️ Stopping after {chunk_count} chunks")
                    break
            
            # Сохраняем полный ответ в БД
            await self.save_bot_message(message_id, conversation_id, full_response)
            
            # Отправляем завершающий сигнал на фронтенд
            await self.send_done_to_frontend(stream_id, message_id, conversation_id)
            
            logger.info(f"✅ AI processing completed for stream_id: {stream_id}")
            logger.info(f"📊 Total chunks: {chunk_count}")
            logger.info(f"📝 Response length: {len(full_response)} characters")
            
        except Exception as e:
            logger.error(f"💥 AI processing failed for stream_id: {stream_id}, error: {e}")
            import traceback
            traceback.print_exc()
    
    async def send_chunk_to_frontend(self, content, stream_id):
        """Отправка chunk'а на фронтенд"""
        try:
            import aiohttp
            
            # Формируем SSE сообщение
            sse_data = f"data: {json.dumps({'type': 'message', 'content': content})}\n\n"
            
            # Отправляем на фронтенд
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://host.docker.internal:8020/api/chat/stream",
                    data=sse_data,
                    headers={'Content-Type': 'text/plain; charset=utf-8'}
                ) as response:
                    if response.status == 200:
                        logger.debug(f"📤 Chunk sent to frontend: '{content}'")
                    else:
                        logger.warning(f"⚠️ Failed to send chunk to frontend: {response.status}")
                        
        except Exception as e:
            logger.error(f"❌ Error sending chunk to frontend: {e}")
    
    async def send_done_to_frontend(self, stream_id, message_id, conversation_id):
        """Отправка завершающего сигнала на фронтенд"""
        try:
            import aiohttp
            
            # Формируем завершающее SSE сообщение
            done_data = {
                'type': 'done',
                'messageId': str(message_id),
                'conversationId': str(conversation_id),
                'newDocId': None
            }
            sse_data = f"data: {json.dumps(done_data)}\n\n"
            
            # Отправляем на фронтенд
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://host.docker.internal:8020/api/chat/stream",
                    data=sse_data,
                    headers={'Content-Type': 'text/plain; charset=utf-8'}
                ) as response:
                    if response.status == 200:
                        logger.info(f"✅ Done signal sent to frontend")
                    else:
                        logger.warning(f"⚠️ Failed to send done signal: {response.status}")
                        
        except Exception as e:
            logger.error(f"❌ Error sending done signal: {e}")
    
    async def save_bot_message(self, message_id, conversation_id, response_text):
        """Сохранение ответа бота в БД"""
        try:
            from django.apps import apps
            from asgiref.sync import sync_to_async
            
            # Получаем модели
            Message = apps.get_model('chat', 'Message')
            Conversation = apps.get_model('chat', 'Conversation')
            
            # Находим беседу
            conversation = await sync_to_async(Conversation.objects.get)(
                conversation_id=conversation_id
            )
            
            # Получаем следующий message_id для этой беседы
            last_message = await sync_to_async(Message.objects.filter)(
                conversation_id=conversation_id
            ).order_by('-message_id').first()
            
            next_message_id = 1 if not last_message else last_message.message_id + 1
            
            # Создаем сообщение бота
            bot_message = await sync_to_async(Message.objects.create)(
                conversation=conversation.id,  # Используем глобальный ID
                conversation_id=conversation_id,
                message_id=next_message_id,
                message=response_text,
                is_bot=True,
                message_type=0,
                tokens=0
            )
            
            logger.info(f"✅ Bot message saved successfully with ID: {bot_message.id}")
            logger.info(f"✅ Bot message saved with ID: {bot_message.id}")
            
        except Exception as e:
            logger.error(f"❌ Error saving bot message: {e}")
            import traceback
            traceback.print_exc()
    
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
