# 🚀 Руководство по интеграции фронтенда с Kafka Consumer

## 📋 **Что уже реализовано на бэкенде:**

### 1. **Kafka Consumer Service** (`chat/kafka_consumer_service_simple.py`)
- ✅ **Получает сообщения** из Kafka топика `chat-ui-messages`
- ✅ **Вызывает LLM** через `api/conversation_cloud`
- ✅ **Получает SSE поток** от LLM
- ✅ **Передает поток на фронтенд** через HTTP POST запрос

### 2. **Поток данных:**
```
Пользователь → Gateway → Chat Service → Kafka → Consumer → LLM → SSE поток → Фронтенд
```

### 3. **Что делает Consumer:**
- Читает пользовательское сообщение из Kafka
- Формирует запрос к LLM с правильными параметрами
- Получает полный SSE ответ от LLM
- Отправляет готовый SSE контент на фронтенд через POST запрос

---

## 🎯 **Что нужно реализовать на фронтенде:**

### 1. **Endpoint для приема SSE потока**
Создайте endpoint `/api/chat/stream` который будет принимать POST запросы от Kafka Consumer:

```typescript
// Пример для Express.js
app.post('/api/chat/stream', (req, res) => {
  const { messageId, requestId } = req.headers;
  const sseContent = req.body; // Полный SSE контент от LLM
  
  // Обрабатываем SSE контент
  const parsedContent = parseSSEContent(sseContent);
  
  // Сохраняем ответ от LLM в БД (если нужно)
  saveLLMResponse(messageId, parsedContent);
  
  // Отправляем ответ
  res.status(200).json({ 
    success: true, 
    messageId, 
    requestId,
    contentLength: sseContent.length 
  });
});
```

### 2. **Парсинг SSE контента**
SSE контент приходит в формате:
```
event: message
data: {"content": "Привет"}

event: message
data: {"content": "! Как"}

event: message
data: {"content": " дела?"}

event: done
data: {"messageId": "temp-id", "conversationId": "new", "newDocId": null}
```

```typescript
function parseSSEContent(sseContent: string) {
  const lines = sseContent.split('\n');
  let fullText = '';
  
  for (let i = 0; i < lines.length; i++) {
    if (lines[i].startsWith('data: ')) {
      try {
        const data = JSON.parse(lines[i].substring(6));
        if (data.content) {
          fullText += data.content;
        }
      } catch (e) {
        // Игнорируем некорректные JSON
      }
    }
  }
  
  return fullText;
}
```

### 3. **Сохранение ответа от LLM**
После получения SSE потока, фронтенд может сам решить, как сохранить ответ:

```typescript
async function saveLLMResponse(messageId: string, content: string) {
  try {
    const response = await fetch('/api/chat/messages/', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${getAuthToken()}`
      },
      body: JSON.stringify({
        conversation: getCurrentConversationId(),
        message: content,
        is_bot: true,
        message_type: 0
      })
    });
    
    if (response.ok) {
      console.log('✅ LLM response saved to database');
    }
  } catch (error) {
    console.error('❌ Failed to save LLM response:', error);
  }
}
```

---

## 🔧 **Технические детали:**

### **URL для фронтенда:**
- **Endpoint:** `http://localhost:8020/api/chat/stream`
- **Метод:** POST
- **Content-Type:** `text/event-stream`

### **Заголовки от Consumer:**
- `X-Message-ID`: ID пользовательского сообщения
- `X-Request-ID`: ID запроса для отслеживания
- `Content-Type`: `text/event-stream`
- `Cache-Control`: `no-cache`
- `Connection`: `keep-alive`

### **Тело запроса:**
Полный SSE контент от LLM в виде строки (не JSON).

---

## 📱 **Пример использования на фронтенде:**

```typescript
// 1. Пользователь отправляет сообщение
const userMessage = await sendUserMessage("Привет, как дела?");

// 2. Сообщение сохраняется в БД и отправляется в Kafka
// 3. Kafka Consumer обрабатывает сообщение и вызывает LLM
// 4. LLM возвращает SSE поток
// 5. Consumer отправляет поток на фронтенд через POST /api/chat/stream
// 6. Фронтенд получает поток, парсит его и показывает пользователю
// 7. Фронтенд может сохранить ответ от LLM в БД

// Обработка на фронтенде
app.post('/api/chat/stream', async (req, res) => {
  const sseContent = req.body;
  const messageId = req.headers['x-message-id'];
  
  // Парсим SSE контент
  const llmResponse = parseSSEContent(sseContent);
  
  // Показываем пользователю
  displayLLMResponse(llmResponse);
  
  // Сохраняем в БД (опционально)
  await saveLLMResponse(messageId, llmResponse);
  
  res.status(200).json({ success: true });
});
```

---

## 🎉 **Преимущества нового подхода:**

1. **🎯 Разделение ответственности** - каждый сервис делает свою работу
2. **💾 Гибкость** - фронтенд сам решает, когда и как сохранять ответы
3. **🔧 Простота** - меньше сложной логики в consumer'е
4. **📊 Контроль** - фронтенд может показывать прогресс, обрабатывать ошибки и т.д.
5. **🔄 Надежность** - меньше точек отказа в системе

---

## 🚨 **Важные моменты:**

1. **Фронтенд должен быть доступен** на порту 8020
2. **Endpoint `/api/chat/stream` должен существовать** и принимать POST запросы
3. **SSE контент приходит как строка**, а не как JSON
4. **Фронтенд сам решает**, сохранять ли ответ от LLM в БД
5. **Consumer автоматически перезапускается** при ошибках

---

## 📞 **Поддержка:**

Если возникнут вопросы по интеграции, проверьте:
1. Логи Kafka Consumer: `docker logs chat-service-kafka-consumer`
2. Доступность фронтенда на порту 8020
3. Правильность endpoint'а `/api/chat/stream`
4. Корректность обработки POST запросов
