# 🔧 Technical Implementation Details

## 🏗️ Архитектура системы

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Frontend      │    │   API Gateway    │    │  Chat Service   │
│   (Port 8020)   │◄──►│   (Port 8002)    │◄──►│   (Port 8003)   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   PostgreSQL    │    │      Kafka       │    │ Kafka Consumer  │
│   Database      │◄───│   (Port 9095)    │◄───│   Service       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
                                               ┌─────────────────┐
                                               │   Cloud.ru API  │
                                               │   (External)    │
                                               └─────────────────┘
```

## 📡 Поток данных

### 1. Отправка сообщения пользователя
```
Frontend → API Gateway → Chat Service → PostgreSQL
                                ↓
                            Kafka Producer
```

### 2. Обработка LLM ответа
```
Kafka Consumer → Cloud.ru API → SSE Response → Frontend
```

## 🔑 Ключевые компоненты

### 1. Django Chat Service (`chat/views.py`)
- **MessageViewSet.create()**: Сохраняет сообщение и отправляет в Kafka
- **Kafka Producer**: Отправляет сообщения в топик `chat-ui-messages`

### 2. Kafka Consumer (`chat/kafka_consumer_service_simple.py`)
- **ChatKafkaConsumerService**: Основной класс Consumer
- **_make_cloud_api_request()**: Прямой вызов Cloud.ru API
- **_stream_to_frontend()**: Передача SSE на фронтенд

### 3. Cloud.ru API Integration
- **URL**: `https://foundation-models.api.cloud.ru/v1/chat/completions`
- **Model**: `deepseek-ai/DeepSeek-R1-Distill-Llama-70B`
- **Auth**: Bearer token `YzU1MGExYWYtODhiYy00MWE1LWIzMjAtMWMxMGYxN2IzZGVh.bbf892ff8b0054533d307ed3ba2ffae5`

## 🐳 Docker контейнеры

### Основные сервисы:
- `chat-service-wsgi-postgres`: Django Chat Service
- `chat-service-kafka`: Kafka broker
- `chat-service-kafka-consumer`: Kafka Consumer
- `chat-service-postgres`: PostgreSQL database
- `chat-service-gateway`: API Gateway

### Сетевые настройки:
- **Kafka**: `localhost:9095` (внешний), `chat-service-kafka:29092` (внутренний)
- **PostgreSQL**: `localhost:5432` (внешний), `chat-service-postgres:5432` (внутренний)
- **Chat Service**: `localhost:8003` (внешний), `wsgi-server:8000` (внутренний)

## 📊 Kafka конфигурация

### Топики:
- `chat-ui-messages`: Основной топик для сообщений пользователей

### Consumer Groups:
- `chat-service-consumers-final`: Активная группа Consumer

### Настройки Consumer:
```python
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_GROUP_ID = "chat-service-consumers-final"
KAFKA_TOPIC = "chat-ui-messages"
auto_offset_reset = 'earliest'
enable_auto_commit = True
max_poll_records = 10
```

## 🔐 Аутентификация

### 1. JWT Token (для пользователей)
- **Header**: `Authorization: Bearer <JWT_TOKEN>`
- **Middleware**: `chatgpt_ui_server/middleware.py`
- **Extraction**: Из `sub` claim в JWT

### 2. Internal Key (для межсервисного взаимодействия)
- **Header**: `x-internal-key: gateway-secret-key-2024`
- **Usage**: Внутренние вызовы между сервисами

## 📝 Формат сообщений Kafka

### Структура сообщения:
```json
{
  "operation": "start_stream",
  "request_id": "test-456",
  "payload": {
    "saved_message_id": 999,
    "conversation_id": 8,
    "message": "Текст сообщения пользователя",
    "user_context": {
      "email": "user@example.com",
      "sub": "user-id-123"
    }
  }
}
```

### Поля:
- **`operation`**: Тип операции (`start_stream`)
- **`request_id`**: Уникальный ID запроса
- **`saved_message_id`**: ID сохраненного сообщения в БД
- **`conversation_id`**: ID беседы
- **`message`**: Текст сообщения
- **`user_context`**: Контекст пользователя

## 🌐 Cloud.ru API Request Format

### Запрос:
```json
{
  "model": "deepseek-ai/DeepSeek-R1-Distill-Llama-70B",
  "messages": [
    {
      "role": "system",
      "content": "You are a helpful assistant."
    },
    {
      "role": "user",
      "content": "Текст сообщения пользователя"
    }
  ],
  "temperature": 0.7,
  "top_p": 0.9,
  "max_tokens": 300,
  "frequency_penalty": 0.5,
  "presence_penalty": 0.3,
  "stream": true
}
```

### Ответ (SSE):
```
data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk","created":1234567890,"model":"deepseek-ai/DeepSeek-R1-Distill-Llama-70B","choices":[{"index":0,"delta":{"content":"Привет"},"finish_reason":null}]}

data: {"id":"chatcmpl-xxx","object":"chat.completion.chunk","created":1234567890,"model":"deepseek-ai/DeepSeek-R1-Distill-Llama-70B","choices":[{"index":0,"delta":{"content":"! Как дела?"},"finish_reason":null}]}

event: done
data: {"messageId": "409", "conversationId": "8", "newDocId": null}
```

## 🔄 SSE Streaming на фронтенд

### Endpoint:
```
POST http://localhost:8020/api/chat/stream
```

### Headers:
```
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
X-Message-ID: 409
X-Request-ID: c9284f09-4b1f-4f9d-bbe1-230392bbe904
```

### Body:
Готовый SSE контент от Cloud.ru API

## 🗄️ База данных

### Таблицы:
- **`conversations`**: Беседы пользователей
- **`messages`**: Сообщения в беседах

### Ключевые поля:
- **`conversations.id`**: Глобальный ID беседы
- **`conversations.conversation_id`**: Пользовательский ID беседы
- **`messages.conversation_id`**: Ссылка на `conversations.conversation_id`
- **`messages.is_bot`**: Флаг сообщения бота

## 🚨 Troubleshooting

### 1. Consumer не обрабатывает сообщения
```bash
# Проверить логи Consumer
docker logs chat-service-kafka-consumer --tail=50

# Проверить сообщения в топике
docker exec chat-service-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic chat-ui-messages --from-beginning --max-messages 5
```

### 2. Ошибки Cloud.ru API
```bash
# Проверить логи Consumer на ошибки API
docker logs chat-service-kafka-consumer | grep "Cloud.ru API"
```

### 3. Проблемы с SSE
```bash
# Проверить логи Consumer на SSE ошибки
docker logs chat-service-kafka-consumer | grep "SSE"
```

### 4. Проблемы с базой данных
```bash
# Проверить подключение к БД
docker exec chat-service-postgres psql -U chat_service_user -d chat_service_db -c "SELECT COUNT(*) FROM messages;"
```

## 📈 Мониторинг

### Ключевые метрики:
- Количество сообщений в Kafka топике
- Время обработки Consumer
- Успешность вызовов Cloud.ru API
- Время ответа SSE потока

### Логи для мониторинга:
- `docker logs chat-service-kafka-consumer`
- `docker logs chat-service-wsgi-postgres`
- `docker logs chat-service-kafka`

## 🔧 Конфигурация

### Environment Variables:
- `KAFKA_BOOTSTRAP_SERVERS`: Адрес Kafka
- `DATABASE_URL`: URL PostgreSQL
- `JWT_SECRET`: Секрет для JWT
- `INTERNAL_KEY`: Ключ для межсервисного взаимодействия

### Docker Compose:
- `docker-compose-postgres.yml`: Основная конфигурация
- `docker-compose-kafka.yml`: Конфигурация Kafka

---

**Техническая документация готова!** Используйте эти детали для понимания внутренней работы системы.
