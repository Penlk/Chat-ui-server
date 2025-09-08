#!/usr/bin/env python
import requests
import json

def test_stream_api():
    """Тестируем stream API с cloud.ru"""
    
    url = "http://127.0.0.1:8003/api/chat/conversation/"
    
    headers = {
        "Content-Type": "application/json",
        "X-User-Data": json.dumps({
            "jwt_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0LXN1Yi0xMjMiLCJlbWFpbCI6InRlc3RAZXhhbXBsZS5jb20iLCJmdWxsX25hbWUiOiJUZXN0IFVzZXIiLCJvcmdfaWQiOiJ0ZXN0LW9yZy0xMjMiLCJyb2xlcyI6WyJ1c2VyIl0sImlhdCI6MTYzNDU2Nzg5MCwiZXhwIjoxOTQ5OTI3ODkwfQ.test-signature",
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
        "name": "gpt-3.5-turbo",
        "message": [
            {
                "role": "user",
                "content": "Привет! Расскажи мне о погоде в Москве"
            }
        ],
        "conversationId": 29,
        "temperature": 0.7
    }
    
    print("🚀 Отправляем запрос к stream API...")
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    print(f"Data: {json.dumps(data, indent=2, ensure_ascii=False)}")
    
    try:
        response = requests.post(url, headers=headers, json=data, stream=True)
        
        print(f"\n📡 Статус ответа: {response.status_code}")
        print(f"📡 Заголовки ответа: {dict(response.headers)}")
        
        if response.status_code == 200:
            print("\n📨 Получаем stream ответ:")
            for line in response.iter_lines():
                if line:
                    line_str = line.decode('utf-8')
                    print(f"📨 {line_str}")
                    
                    # Парсим SSE формат
                    if line_str.startswith('data: '):
                        data_str = line_str[6:]  # Убираем 'data: '
                        try:
                            data_json = json.loads(data_str)
                            if 'content' in data_json:
                                print(f"💬 Контент: {data_json['content']}")
                        except json.JSONDecodeError:
                            print(f"❌ Не удалось распарсить JSON: {data_str}")
        else:
            print(f"❌ Ошибка: {response.text}")
            
    except Exception as e:
        print(f"❌ Исключение: {e}")

if __name__ == "__main__":
    test_stream_api()
