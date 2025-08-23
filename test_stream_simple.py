#!/usr/bin/env python
import requests
import json

def test_stream_simple():
    """Тестируем stream API без работы с базой данных"""
    
    url = "http://localhost:8003/api/conversation_cloud/"
    
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
    
    # Используем точно те же параметры, что и в рабочем test_cloud_api.py
    data = {
        "name": "deepseek-ai/DeepSeek-R1-Distill-Llama-70B",
        "message": [
            {
                "role": "user",
                "content": "Hello! Tell me about the weather in Moscow"
            }
        ],
        "temperature": 0.7,
        "top_p": 0.9,
        "max_tokens": 300,
        "frequency_penalty": 0.5,
        "presence_penalty": 0.3
    }
    
    print("🚀 Тестируем stream API...")
    print(f"URL: {url}")
    print(f"Data: {json.dumps(data, indent=2, ensure_ascii=False)}")
    
    try:
        response = requests.post(url, headers=headers, json=data, stream=True)
        
        print(f"\n📡 Статус ответа: {response.status_code}")
        print(f"📡 Content-Type: {response.headers.get('Content-Type', 'Не указан')}")
        
        if response.status_code == 200:
            print("\n📨 Получаем stream ответ:")
            full_content = ""
            
            for line in response.iter_lines():
                if line:
                    line_str = line.decode('utf-8')
                    print(f"📨 {line_str}")
                    
                    # Парсим SSE формат
                    if line_str.startswith('data: '):
                        data_str = line_str[6:]  # Убираем 'data: '
                        if data_str.strip() == '[DONE]':
                            print("✅ Stream завершен")
                            break
                        try:
                            data_json = json.loads(data_str)
                            if 'content' in data_json:
                                content = data_json['content']
                                full_content += content
                                print(f"💬 Контент: {content}")
                        except json.JSONDecodeError:
                            print(f"❌ Не удалось распарсить JSON: {data_str}")
            
            print(f"\n📝 Полный ответ: {full_content}")
        else:
            print(f"❌ Ошибка: {response.text}")
            
    except Exception as e:
        print(f"❌ Исключение: {e}")

if __name__ == "__main__":
    test_stream_simple()
