#!/usr/bin/env python3
"""
Kafka Consumer Service для Chat Service (без Django)
Запускается как отдельный процесс для обработки сообщений из топика chat-ui-messages
"""
import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Dict, Any
import aiohttp

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', "kafka:29092")
KAFKA_GROUP_ID = "chat-service-consumers-final"
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
        self.message_collector = MessageCollector()
        
    async def start(self):
        """Запуск Kafka consumer"""
        try:
            logger.info(f"🚀 Starting Chat Kafka Consumer Service...")
            logger.info(f"📡 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            logger.info(f"🎯 Topic: {KAFKA_TOPIC}")
            logger.info(f"👥 Consumer group: {KAFKA_GROUP_ID}")
            
            # Импортируем AIOKafkaConsumer здесь, чтобы избежать проблем с зависимостями
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
            import traceback
            logger.error(f"💥 Full traceback: {traceback.format_exc()}")
    
    async def stop(self):
        """Остановка Kafka consumer"""
        self.running = False
        if self.consumer:
            await self.consumer.stop()
            logger.info("🛑 Kafka consumer stopped")
    
    async def _consume_messages(self):
        """Основной цикл потребления сообщений"""
        try:
            async for message in self.consumer:
                if not self.running:
                    break
                    
                try:
                    # Получаем полное сообщение
                    message_str = message.value
                    logger.info(f"📬 Received message: {message_str}")
                    
                    # Парсим JSON
                    try:
                        message_data = json.loads(message_str)
                        logger.info(f"📊 Parsed message data: {message_data}")
                        
                        # Обрабатываем сообщение
                        await self._process_message_data(message_data)
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ Failed to parse JSON: {e}")
                        logger.error(f"❌ Raw message: {message_str}")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}")
                    
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
            logger.info(f"🔧 Starting _handle_start_stream")
            
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
            
            logger.info(f"📞 About to call _call_conversation_cloud...")
            
            # Вызываем api/conversation_cloud для streaming
            await self._call_conversation_cloud(
                message_id=saved_message_id,
                conversation_id=conversation_id,
                message=message_text,
                user_context=user_context,
                request_id=request_id
            )
            
            logger.info(f"✅ _call_conversation_cloud completed")
            
        except Exception as e:
            logger.error(f"💥 Error in _handle_start_stream: {e}")
            import traceback
            logger.error(f"💥 Full traceback: {traceback.format_exc()}")
    
    async def _call_conversation_cloud(self, message_id, conversation_id, message, user_context, request_id):
        """Прямой вызов Cloud.ru API для streaming"""
        try:
            logger.info(f"🔧 Starting direct Cloud.ru API call for message {message_id}")
            
            # Данные для запроса к Cloud.ru API
            cloud_data = {
                "model": "deepseek-ai/DeepSeek-R1-Distill-Llama-70B",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a helpful assistant."
                    },
                    {
                        "role": "user",
                        "content": message
                    }
                ],
                "temperature": 0.7,
                "top_p": 0.9,
                "max_tokens": 300,
                "frequency_penalty": 0.5,
                "presence_penalty": 0.3,
                "stream": True
            }
            
            # API ключ для Cloud.ru
            api_key = "YzU1MGExYWYtODhiYy00MWE1LWIzMjAtMWMxMGYxN2IzZGVh.bbf892ff8b0054533d307ed3ba2ffae5"
            
            logger.info(f"📡 Calling Cloud.ru API directly for message {message_id}")
            logger.info(f"📤 Request data: {cloud_data}")
            
            # Выполняем прямой запрос к Cloud.ru API
            logger.info(f"🌐 Making direct request to Cloud.ru API...")
            response = await self._make_cloud_api_request(cloud_data, api_key)
            
            if response:
                logger.info(f"✅ Got response from Cloud.ru API")
                sse_content = response.get("content", "")
                
                if sse_content:
                    # Передаем содержимое на фронтенд (фронтенд сам решит, как сохранить)
                    await self._stream_to_frontend(sse_content, message_id, request_id)
                else:
                    logger.warning(f"⚠️ No content in response")
            else:
                logger.warning(f"⚠️ No response from Cloud.ru API")
            
            logger.info(f"✅ AI processing completed for message {message_id}")
            logger.info(f"   - Request ID: {request_id}")
            logger.info(f"   - Conversation ID: {conversation_id}")
            logger.info(f"   - User: {user_context.get('email')}")
            
        except Exception as e:
            logger.error(f"💥 Error calling Cloud.ru API: {e}")
            import traceback
            logger.error(f"💥 Full traceback: {traceback.format_exc()}")
    

    async def _make_cloud_api_request(self, data, api_key):
        """Прямой запрос к Cloud.ru API"""
        try:
            url = "https://foundation-models.api.cloud.ru/v1/chat/completions"
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            }
            
            logger.info(f"🌐 Making direct request to Cloud.ru API: {url}")
            logger.info(f"📤 Headers: {dict(headers)}")
            
            # Создаем сессию с таймаутом
            timeout = aiohttp.ClientTimeout(total=120)  # 2 минуты на весь запрос
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=data, headers=headers) as response:
                    if response.status == 200:
                        logger.info(f"✅ Received SSE response from Cloud.ru API")
                        
                        # Читаем содержимое с таймаутом
                        try:
                            # Читаем ответ по частям, чтобы избежать зависания
                            sse_content = ""
                            async for line in response.content:
                                line_str = line.decode('utf-8').strip()
                                if line_str:
                                    sse_content += line_str + "\n"
                                    logger.info(f"📋 Получена строка: {line_str[:100]}...")
                                
                                # Проверяем, завершился ли ответ
                                if "event: done" in line_str or "[DONE]" in line_str:
                                    logger.info(f"✅ Получен сигнал завершения")
                                    break
                            
                            logger.info(f"📋 Полный ответ от Cloud.ru API (длина: {len(sse_content)} символов)")
                            return {"content": sse_content, "response": response}
                            
                        except asyncio.TimeoutError:
                            logger.error(f"💥 Timeout while reading SSE response")
                            return None
                        except Exception as e:
                            logger.error(f"💥 Error reading response content: {e}")
                            return None
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ Cloud.ru API error: {response.status} - {error_text}")
                        return None
                        
        except asyncio.TimeoutError:
            logger.error(f"💥 Timeout while making Cloud.ru API request")
            return None
        except Exception as e:
            logger.error(f"💥 Error making Cloud.ru API request: {e}")
            return None
    
    async def _stream_to_frontend(self, sse_content, message_id, request_id):
        """Передача SSE контента на фронтенд"""
        try:
            frontend_url = "http://host.docker.internal:8020/api/chat/stream"
            headers = {
                'Content-Type': 'text/event-stream',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Message-ID': str(message_id),
                'X-Request-ID': str(request_id)
            }
            
            logger.info(f"📤 Streaming to frontend: {frontend_url}")
            logger.info(f"📤 Content length: {len(sse_content)} characters")
            
            # Передаем готовый SSE контент на фронтенд
            async with aiohttp.ClientSession() as session:
                async with session.post(frontend_url, headers=headers, data=sse_content) as frontend_response:
                    if frontend_response.status == 200:
                        logger.info(f"✅ Successfully streamed to frontend")
                    else:
                        error_text = await frontend_response.text()
                        logger.error(f"❌ Frontend streaming error: {frontend_response.status} - {error_text}")
                        
        except Exception as e:
            logger.error(f"💥 Error streaming to frontend: {e}")

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
