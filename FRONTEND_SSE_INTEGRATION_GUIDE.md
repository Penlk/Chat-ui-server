# 🚀 Frontend SSE Integration Guide

## 📋 Обзор архитектуры

Наша система теперь работает следующим образом:

1. **Пользователь отправляет сообщение** → Django Chat Service (порт 8003)
2. **Django сохраняет сообщение** → PostgreSQL Database
3. **Django отправляет в Kafka** → Топик `chat-ui-messages`
4. **Kafka Consumer обрабатывает** → Вызывает Cloud.ru API напрямую
5. **Cloud.ru API возвращает SSE** → Consumer получает streaming ответ
6. **Consumer передает на фронтенд** → SSE endpoint на порту 8020

## 🔗 Endpoints для фронтенда

### 1. Отправка сообщения
```
POST http://localhost:8002/api/chat/messages/
```

**Headers:**
```
Authorization: Bearer <JWT_TOKEN>
Content-Type: application/json
```

**Body:**
```json
{
  "conversation": 8,
  "message": "Привет, как дела?",
  "is_bot": false,
  "message_type": 0
}
```

**Response:**
```json
{
  "id": 409,
  "conversation": 8,
  "message": "Привет, как дела?",
  "is_bot": false,
  "message_type": 0,
  "created_at": "2025-09-04T18:28:21.123456Z"
}
```

### 2. Получение SSE потока
```
POST http://localhost:8020/api/chat/stream
```

**Headers:**
```
Content-Type: text/event-stream
Cache-Control: no-cache
Connection: keep-alive
X-Message-ID: 409
X-Request-ID: c9284f09-4b1f-4f9d-bbe1-230392bbe904
```

**Body:** SSE контент от Cloud.ru API

## 📡 Формат SSE ответа от Cloud.ru API

### Структура SSE событий:

```
data: {"id":"chatcmpl-784fbf55-ec41-487c-a33d-ecc72e423efc","object":"chat.completion.chunk","created":1725463423,"model":"deepseek-ai/DeepSeek-R1-Distill-Llama-70B","choices":[{"index":0,"delta":{"content":"Привет"},"finish_reason":null}]}

data: {"id":"chatcmpl-784fbf55-ec41-487c-a33d-ecc72e423efc","object":"chat.completion.chunk","created":1725463423,"model":"deepseek-ai/DeepSeek-R1-Distill-Llama-70B","choices":[{"index":0,"delta":{"content":"! Как"},"finish_reason":null}]}

data: {"id":"chatcmpl-784fbf55-ec41-487c-a33d-ecc72e423efc","object":"chat.completion.chunk","created":1725463423,"model":"deepseek-ai/DeepSeek-R1-Distill-Llama-70B","choices":[{"index":0,"delta":{"content":" дела?"},"finish_reason":null}]}

event: done
data: {"messageId": "409", "conversationId": "8", "newDocId": null}
```

### Поля в SSE данных:

- **`id`**: Уникальный ID ответа от Cloud.ru API
- **`object`**: Тип объекта (`chat.completion.chunk`)
- **`created`**: Timestamp создания
- **`model`**: Модель, которая генерировала ответ
- **`choices[0].delta.content`**: Фрагмент текста ответа
- **`choices[0].finish_reason`**: Причина завершения (`null` для продолжения, `"stop"` для завершения)

## 🛠️ Реализация на фронтенде

### 1. Отправка сообщения

```javascript
async function sendMessage(conversationId, messageText) {
  const response = await fetch('http://localhost:8002/api/chat/messages/', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${jwtToken}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      conversation: conversationId,
      message: messageText,
      is_bot: false,
      message_type: 0
    })
  });
  
  if (response.ok) {
    const messageData = await response.json();
    console.log('Message saved:', messageData);
    return messageData;
  } else {
    throw new Error(`Failed to send message: ${response.status}`);
  }
}
```

### 2. Получение SSE потока

```javascript
async function listenForSSEStream(messageId, requestId) {
  const eventSource = new EventSource(`http://localhost:8020/api/chat/stream`, {
    headers: {
      'X-Message-ID': messageId,
      'X-Request-ID': requestId
    }
  });
  
  let fullResponse = '';
  
  eventSource.onmessage = function(event) {
    try {
      const data = JSON.parse(event.data);
      
      if (data.choices && data.choices[0] && data.choices[0].delta.content) {
        const content = data.choices[0].delta.content;
        fullResponse += content;
        
        // Обновляем UI с новым фрагментом
        updateChatUI(content);
      }
    } catch (e) {
      console.error('Error parsing SSE data:', e);
    }
  };
  
  eventSource.addEventListener('done', function(event) {
    const doneData = JSON.parse(event.data);
    console.log('Stream completed:', doneData);
    
    // Сохраняем полный ответ в базу данных
    saveBotMessage(messageId, fullResponse);
    
    eventSource.close();
  });
  
  eventSource.onerror = function(event) {
    console.error('SSE error:', event);
    eventSource.close();
  };
  
  return eventSource;
}
```

### 3. Полный workflow

```javascript
async function sendMessageAndGetResponse(conversationId, messageText) {
  try {
    // 1. Отправляем сообщение пользователя
    const userMessage = await sendMessage(conversationId, messageText);
    console.log('User message saved:', userMessage);
    
    // 2. Начинаем слушать SSE поток
    const eventSource = await listenForSSEStream(userMessage.id, userMessage.id);
    
    // 3. Обрабатываем ответ
    eventSource.addEventListener('done', async function(event) {
      const doneData = JSON.parse(event.data);
      
      // 4. Сохраняем ответ бота в базу данных
      await saveBotMessage(userMessage.id, fullResponse);
      
      console.log('Complete workflow finished');
    });
    
  } catch (error) {
    console.error('Error in message workflow:', error);
  }
}
```

## 🔧 Настройка фронтенда

### 1. CORS настройки

Убедитесь, что ваш фронтенд может обращаться к:
- `http://localhost:8002` (API Gateway)
- `http://localhost:8020` (SSE endpoint)

### 2. Обработка ошибок

```javascript
function handleSSEError(event) {
  if (event.readyState === EventSource.CLOSED) {
    console.log('SSE connection closed');
  } else if (event.readyState === EventSource.CONNECTING) {
    console.log('SSE reconnecting...');
  }
}
```

### 3. Таймауты

```javascript
// Устанавливаем таймаут для SSE соединения
setTimeout(() => {
  if (eventSource.readyState !== EventSource.CLOSED) {
    console.log('SSE timeout, closing connection');
    eventSource.close();
  }
}, 300000); // 5 минут
```

## 📊 Мониторинг и отладка

### Логи Consumer

```bash
docker logs chat-service-kafka-consumer --tail=50
```

### Проверка сообщений в Kafka

```bash
docker exec chat-service-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic chat-ui-messages --from-beginning --max-messages 5
```

### Проверка сохраненных сообщений

```bash
docker exec chat-service-postgres psql -U chat_service_user -d chat_service_db -c "SELECT id, conversation_id, message, is_bot, created_at FROM messages ORDER BY created_at DESC LIMIT 10;"
```

## 🚨 Важные моменты

1. **JWT Token**: Всегда передавайте JWT token в заголовке `Authorization: Bearer <token>`
2. **Message ID**: Используйте ID сообщения для связи между запросом и ответом
3. **SSE Format**: Cloud.ru API возвращает стандартный SSE формат с `data:` префиксом
4. **Error Handling**: Всегда обрабатывайте ошибки соединения и таймауты
5. **Content Assembly**: Собирайте фрагменты ответа из `choices[0].delta.content`

## 🔄 Полный пример интеграции

```javascript
class ChatService {
  constructor(jwtToken) {
    this.jwtToken = jwtToken;
    this.baseURL = 'http://localhost:8002';
    this.sseURL = 'http://localhost:8020';
  }
  
  async sendMessage(conversationId, messageText) {
    const response = await fetch(`${this.baseURL}/api/chat/messages/`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${this.jwtToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        conversation: conversationId,
        message: messageText,
        is_bot: false,
        message_type: 0
      })
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${await response.text()}`);
    }
    
    return await response.json();
  }
  
  async listenForResponse(messageId) {
    return new Promise((resolve, reject) => {
      const eventSource = new EventSource(`${this.sseURL}/api/chat/stream`, {
        headers: {
          'X-Message-ID': messageId,
          'X-Request-ID': messageId
        }
      });
      
      let fullResponse = '';
      
      eventSource.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.choices?.[0]?.delta?.content) {
            fullResponse += data.choices[0].delta.content;
            this.onContentUpdate?.(data.choices[0].delta.content);
          }
        } catch (e) {
          console.error('SSE parse error:', e);
        }
      };
      
      eventSource.addEventListener('done', (event) => {
        eventSource.close();
        resolve(fullResponse);
      });
      
      eventSource.onerror = (error) => {
        eventSource.close();
        reject(error);
      };
    });
  }
  
  async sendMessageAndGetResponse(conversationId, messageText) {
    const userMessage = await this.sendMessage(conversationId, messageText);
    const botResponse = await this.listenForResponse(userMessage.id);
    return { userMessage, botResponse };
  }
}

// Использование
const chatService = new ChatService('your-jwt-token');
chatService.onContentUpdate = (content) => {
  console.log('New content:', content);
};

chatService.sendMessageAndGetResponse(8, 'Привет!')
  .then(({ userMessage, botResponse }) => {
    console.log('User message:', userMessage);
    console.log('Bot response:', botResponse);
  })
  .catch(console.error);
```

## 📝 Changelog

- **2025-09-04**: Убрали промежуточный `api/conversation_cloud` endpoint
- **2025-09-04**: Реализовали прямой вызов Cloud.ru API из Kafka Consumer
- **2025-09-04**: Настроили SSE streaming от Consumer к фронтенду
- **2025-09-04**: Исправили проблемы с обработкой сообщений в Consumer

---

**Готово!** Теперь ваш фронтенд может интегрироваться с нашей системой через SSE поток.
