# 📋 Документация для Chat Service - Kafka Integration

## 🎯 Обзор интеграции

API Gateway теперь использует **гибридный подход** для обработки сообщений:
1. **HTTP** - для мгновенного сохранения сообщения в БД
2. **Kafka** - для запуска AI streaming обработки

## 🔧 Конфигурация Kafka

### Подключение к Kafka
```python
# Настройки Kafka для Chat Service
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"  # Или ваш Kafka сервер
KAFKA_GROUP_ID = "chat-service-consumers"
KAFKA_TOPIC = "chat-ui-messages"
```

### Инициализация Consumer
```python
from aiokafka import AIOKafkaConsumer
import json

# Создание consumer
consumer = AIOKafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id=KAFKA_GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Запуск consumer
await consumer.start()
```

## 📨 Структура сообщения в Kafka

### Формат сообщения
```json
{
  "message_id": "1cc27d3b-5160-442e-90d9-2ca2359c127a",
  "request_id": "81111e8c-4d8b-4f4b-8482-2dc6f9e70457",
  "timestamp": "2025-09-01T10:20:33.154427Z",
  "payload": {
    "conversation_id": 1,
    "message": "Текст сообщения пользователя",
    "is_bot": false,
    "message_type": 0,
    "saved_message_id": 243,  // ID сохраненного в БД сообщения
    "message_id": 1,          // message_id из БД
    "user_context": {
      "email": "test4@example.com",
      "full_name": "Test User 4",
      "active_org_id": null,
      "org_role": "user",
      "is_org_owner": false
    },
    "request_metadata": {
      "source_ip": "172.18.0.1",
      "user_agent": "Mozilla/5.0 (Windows NT 10.0; ru-RU) WindowsPowerShell/5.1.26100.4768"
    }
  },
  "operation": "start_stream"
}
```

### Поля сообщения
| Поле | Тип | Описание |
|------|-----|----------|
| `message_id` | string | Уникальный ID сообщения в Kafka |
| `request_id` | string | ID запроса для отслеживания |
| `timestamp` | string | Время создания сообщения |
| `payload.conversation_id` | int | ID разговора |
| `payload.message` | string | Текст сообщения |
| `payload.is_bot` | boolean | Сообщение от бота |
| `payload.message_type` | int | Тип сообщения |
| `payload.saved_message_id` | int | ID сохраненного сообщения в БД |
| `payload.message_id` | int | message_id из БД |
| `payload.user_context` | object | Контекст пользователя |
| `payload.request_metadata` | object | Метаданные запроса |
| `operation` | string | Операция ("start_stream") |

## 🔄 Логика обработки в Chat Service

### Основной обработчик
```python
async def process_stream_message(message_data):
    """Обработка сообщения для запуска stream"""
    
    # 1. Извлекаем данные
    operation = message_data.get("operation")
    payload = message_data.get("payload", {})
    request_id = message_data.get("request_id")
    
    if operation == "start_stream":
        # 2. Получаем данные сообщения
        saved_message_id = payload.get("saved_message_id")
        conversation_id = payload.get("conversation_id")
        message_text = payload.get("message")
        user_context = payload.get("user_context", {})
        
        # 3. Запускаем AI обработку / streaming
        await start_ai_processing(
            message_id=saved_message_id,
            conversation_id=conversation_id,
            user_message=message_text,
            user_email=user_context.get("email"),
            stream_id=request_id
        )
```

### AI обработка с streaming
```python
async def start_ai_processing(message_id, conversation_id, user_message, user_email, stream_id):
    """Запуск AI обработки с streaming на фронтенд"""
    
    try:
        # 1. Получение контекста разговора из БД
        conversation = await get_conversation(conversation_id)
        messages_history = await get_conversation_messages(conversation_id)
        
        # 2. Вызов AI API с streaming
        async for chunk in ai_client.stream_response(
            messages=messages_history,
            new_message=user_message
        ):
            # 3. Отправка chunk'ов через WebSocket/SSE на фронтенд
            await send_stream_chunk(
                stream_id=stream_id,
                user_email=user_email,
                chunk=chunk
            )
        
        # 4. Сохранение ответа AI в БД
        ai_response = "Полный ответ AI"
        await save_ai_response(conversation_id, ai_response)
        
        # 5. Логирование успеха
        logger.info(f"Stream completed for request_id: {stream_id}")
        
    except Exception as e:
        # 6. Обработка ошибок
        logger.error(f"Stream failed for request_id: {stream_id}, error: {e}")
        await send_stream_error(stream_id, user_email, str(e))
```

### Consumer loop
```python
async def consume_messages():
    """Основной цикл потребления сообщений из Kafka"""
    
    try:
        async for message in consumer:
            try:
                # Обработка сообщения
                await process_stream_message(message.value)
                
                # Подтверждение обработки
                await consumer.commit()
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Можно добавить retry логику или отправку в dead letter queue
                
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        await consumer.stop()

# Запуск consumer
if __name__ == "__main__":
    asyncio.run(consume_messages())
```

## 🌐 Топики для ответов (опционально)

Если Chat Service должен отвечать обратно в Gateway:

### Настройка Producer
```python
from aiokafka import AIOKafkaProducer

# Producer для ответов
producer = AIOKafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

await producer.start()
```

### Отправка ответа
```python
async def send_response_to_gateway(original_request_id, status, payload):
    """Отправка ответа обратно в Gateway"""
    
    response_topic = "chat-service-responses"
    
    response_message = {
        "request_id": original_request_id,
        "status": status,  # "success" или "error"
        "payload": payload,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    await producer.send(response_topic, response_message)
    
# Пример использования
await send_response_to_gateway(
    request_id="81111e8c-4d8b-4f4b-8482-2dc6f9e70457",
    status="success",
    payload={
        "stream_started": True,
        "stream_url": f"ws://localhost:8003/stream/{stream_id}",
        "message_processed": True
    }
)
```

## ✅ Преимущества гибридного подхода

1. **🚀 Мгновенное сохранение** - сообщение сохраняется через HTTP
2. **🔄 Надежный stream** - Kafka гарантирует доставку для streaming
3. **📊 Полный контекст** - Chat Service получает всю информацию о пользователе
4. **🛡️ Отказоустойчивость** - если Kafka недоступна, основная функциональность работает
5. **📈 Масштабируемость** - можно добавлять consumer'ы для разных типов обработки
6. **⚡ Низкая задержка** - HTTP для быстрого сохранения, Kafka для асинхронной обработки

## 🧪 Тестирование интеграции

### Проверка топика
```bash
# Список всех топиков
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Информация о топике
docker exec kafka kafka-topics --describe --topic chat-ui-messages --bootstrap-server localhost:9092
```

### Просмотр сообщений
```bash
# Просмотр всех сообщений
docker exec kafka kafka-console-consumer --topic chat-ui-messages --bootstrap-server localhost:9092 --from-beginning

# Просмотр последних сообщений
docker exec kafka kafka-console-consumer --topic chat-ui-messages --bootstrap-server localhost:9092 --max-messages 5
```

### Отправка тестового сообщения
```bash
# Отправка тестового сообщения
docker exec kafka kafka-console-producer --topic chat-ui-messages --bootstrap-server localhost:9092
```

## 🔍 Мониторинг и логирование

### Логирование в Chat Service
```python
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Логирование событий
logger.info(f"Received message from Kafka: {message_data}")
logger.info(f"Starting AI processing for request_id: {request_id}")
logger.info(f"Stream completed for request_id: {request_id}")
logger.error(f"Stream failed for request_id: {request_id}, error: {e}")
```

### Метрики для мониторинга
```python
# Счетчики для мониторинга
messages_received = 0
streams_started = 0
streams_completed = 0
streams_failed = 0

# Обновление метрик
messages_received += 1
streams_started += 1
streams_completed += 1
```

## 🚀 Развертывание

### Docker Compose (пример)
```yaml
version: '3.8'
services:
  chat-service:
    build: .
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:29092
      - KAFKA_GROUP_ID=chat-service-consumers
      - KAFKA_TOPIC=chat-ui-messages
    depends_on:
      - kafka
    networks:
      - app-network

  kafka:
    image: confluentinc/cp-kafka:latest
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    networks:
      - app-network
```

### Переменные окружения
```bash
# Обязательные
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_GROUP_ID=chat-service-consumers
KAFKA_TOPIC=chat-ui-messages

# Опциональные
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_ENABLE_AUTO_COMMIT=true
KAFKA_MAX_POLL_RECORDS=500
```

## 📝 Чек-лист внедрения

- [ ] Настроить подключение к Kafka
- [ ] Создать consumer для топика `chat-ui-messages`
- [ ] Реализовать обработчик сообщений
- [ ] Добавить AI streaming логику
- [ ] Настроить WebSocket/SSE для фронтенда
- [ ] Добавить обработку ошибок
- [ ] Настроить логирование
- [ ] Протестировать интеграцию
- [ ] Настроить мониторинг
- [ ] Документировать API для фронтенда

## 🆘 Устранение неполадок

### Частые проблемы

1. **Kafka недоступен**
   - Проверить подключение: `telnet kafka 29092`
   - Проверить статус контейнера: `docker ps | grep kafka`

2. **Сообщения не доходят**
   - Проверить топик: `kafka-topics --describe --topic chat-ui-messages`
   - Проверить consumer group: `kafka-consumer-groups --describe --group chat-service-consumers`

3. **Ошибки десериализации**
   - Проверить формат JSON в сообщениях
   - Убедиться в правильной кодировке UTF-8

4. **Stream не запускается**
   - Проверить логи Chat Service
   - Убедиться в корректности `operation: "start_stream"`

---

**Готово!** Chat Service теперь может подписаться на топик `chat-ui-messages` и получать все сообщения для запуска streaming обработки! 🎉
