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
        """Вызов cloud.ru API для streaming"""
        try:
            logger.info(f"📡 Calling cloud.ru API for message {message_id}")
            
            # Получаем API ключ из переменных окружения
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
                {"role": "user", "content": message}
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
                            await self._send_chunk_to_frontend(content, request_id)
                            
                        # Обрабатываем content (стандартный OpenAI)
                        elif hasattr(delta, 'content') and delta.content:
                            content = delta.content
                            full_response += content
                            
                            # Отправляем chunk на фронтенд
                            await self._send_chunk_to_frontend(content, request_id)
                
                # Ограничиваем количество chunk'ов для тестирования
                if chunk_count >= 100:  # Увеличиваем лимит для полного ответа
                    logger.info(f"⏹️ Stopping after {chunk_count} chunks")
                    break
            
            # Сохраняем полный ответ в БД
            await self._save_bot_message(message_id, conversation_id, full_response)
            
            # Отправляем завершающий сигнал на фронтенд
            await self._send_done_to_frontend(request_id, message_id, conversation_id)
            
            logger.info(f"✅ AI processing completed for message {message_id}")
            logger.info(f"   - Request ID: {request_id}")
            logger.info(f"   - Conversation ID: {conversation_id}")
            logger.info(f"   - User: {user_context.get('email')}")
            logger.info(f"📊 Total chunks: {chunk_count}")
            logger.info(f"📝 Response length: {len(full_response)} characters")
            
        except Exception as e:
            logger.error(f"💥 Error calling cloud.ru API: {e}")
            import traceback
            traceback.print_exc()
    
    async def _send_chunk_to_frontend(self, content, request_id):
        """Отправка chunk'а на фронтенд"""
        try:
            import aiohttp
            
            # Формируем SSE сообщение
            sse_data = f"data: {json.dumps({'type': 'message', 'content': content})}\n\n"
            
            # Отправляем на фронтенд
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://host.docker.internal:8003/internal/stream/",
                    data=sse_data,
                    headers={'Content-Type': 'text/plain; charset=utf-8'}
                ) as response:
                    if response.status == 200:
                        logger.debug(f"📤 Chunk sent to frontend: '{content}'")
                    else:
                        logger.warning(f"⚠️ Failed to send chunk to frontend: {response.status}")
                        
        except Exception as e:
            logger.error(f"❌ Error sending chunk to frontend: {e}")
    
    async def _send_done_to_frontend(self, request_id, message_id, conversation_id):
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
                    f"http://host.docker.internal:8003/internal/stream/",
                    data=sse_data,
                    headers={'Content-Type': 'text/plain; charset=utf-8'}
                ) as response:
                    if response.status == 200:
                        logger.info(f"✅ Done signal sent to frontend")
                    else:
                        logger.warning(f"⚠️ Failed to send done signal: {response.status}")
                        
        except Exception as e:
            logger.error(f"❌ Error sending done signal: {e}")
    
    async def _save_bot_message(self, message_id, conversation_id, response_text):
        """Сохранение ответа бота в БД через HTTP API"""
        try:
            import aiohttp
            
            # Формируем данные для сохранения
            bot_message_data = {
                "message": response_text,
                "conversation_id": conversation_id,
                "is_bot": True,
                "message_type": 0
            }
            
            # Отправляем POST запрос к API для сохранения сообщения
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "http://host.docker.internal:8003/api/chat/messages/",
                    json=bot_message_data,
                    headers={'Content-Type': 'application/json'}
                ) as response:
                    if response.status == 201:
                        result = await response.json()
                        logger.info(f"✅ Bot message saved successfully with ID: {result.get('id')}")
                    else:
                        logger.warning(f"⚠️ Failed to save bot message: {response.status}")
                        
        except Exception as e:
            logger.error(f"❌ Error saving bot message: {e}")
    
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
