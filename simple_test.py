#!/usr/bin/env python
import requests
import json

def test_conversation_cloud():
    print("🚀 Тестируем эндпоинт /api/conversation_cloud/")
    
    url = "http://127.0.0.1:8010/api/conversation_cloud/"
    
    headers = {
        "Content-Type": "application/json",
        "X-User-Data": json.dumps({
            "jwt_token": "test-token",
            "user_data": {
                "sub": "test-sub-123",
                "email": "test@example.com",
                "full_name": "Test User",
                "org_id": "test-org-123",
                "roles": ["user"]
            }
        })
    }
    
    data = {
        "name": "deepseek-ai/DeepSeek-R1-Distill-Llama-70B",
        "message": [
            {
                "role": "user",
                "content": "Привет! Расскажи мне о погоде в Москве"
            }
        ],
        "temperature": 0.7,
        "top_p": 0.9,
        "max_tokens": 100,
        "frequency_penalty": 0.5,
        "presence_penalty": 0.3
    }
    
    print(f"📤 Отправляем запрос на {url}")
    print(f"📤 Данные: {json.dumps(data, indent=2, ensure_ascii=False)}")
    
    try:
        response = requests.post(url, headers=headers, json=data, stream=True, timeout=30)
        
        print(f"📡 Статус ответа: {response.status_code}")
        print(f"📡 Headers: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("✅ Эндпоинт отвечает! Получаем stream данные:")
            
            for line in response.iter_lines():
                if line:
                    line_str = line.decode('utf-8')
                    print(f"📨 {line_str}")
                    
                    # Простая проверка завершения
                    if 'done' in line_str or 'error' in line_str:
                        break
                        
        else:
            print(f"❌ Ошибка HTTP {response.status_code}")
            print(f"❌ Ответ: {response.text}")
            
    except Exception as e:
        print(f"❌ Исключение: {e}")

if __name__ == "__main__":
    test_conversation_cloud()
