#!/usr/bin/env python3
"""
Тестовый скрипт для отправки сообщения на Gateway
"""
import requests
import json

def send_message():
    # URL Gateway
    url = "http://localhost:8002/api/chat/messages/"
    
    # JWT токен
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJVaGtKejZ2TlREWEpnQnoxdGJjSXhjLVc2U0V2NERpQVZVOHFhTGZrckZZIn0.eyJleHAiOjE3NTY3Mjc2NjYsImlhdCI6MTc1NjcyNzM2NiwianRpIjoib25ydHJvOmU0ZmRjYjgxLWE3ZDgtY2MxYS02OWQxLTE0Yjg0MzRlYTk3OCIsImlzcyI6Imh0dHA6Ly9rZXljbG9hazo4MDgwL3JlYWxtcy9hdXRoLXNlcnZpY2UiLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiZWFmNTVhZjktMDQ2Ny00NGRjLThmMTMtOGFmNjg5Yjk3YTA5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYXV0aC1zZXJ2aWNlIiwic2lkIjoiZDI0YmM3YmYtMmRlMC00ZDc2LWE3MGUtZjRkYjFlOGI0ZjZmIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIgaHR0cDovLzEyNy4wLjAuMTo4MDA5Il0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLWF1dGgtc2VydmljZSIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwibmFtZSI6IlRlc3QyIEFjY291bnQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0ZXN0MkBleGFtcGxlLmNvbSIsImdpdmVuX25hbWUiOiJUZXN0MiIsImZhbWlseV9uYW1lIjoiQWNjb3VudCIsImVtYWlsIjoidGVzdDJAZXhhbXBsZS5jb20ifQ.m6o9geUGyW8WRYqsJc8Yb5VHTjyiCUiVJROox2wMlEyJ6EhbABFCe_UVBLXK8FszxQtjVQGsfAlocK3unNbuTD9X8OD4HDJLZvtfBkkqiyLWDN88cbUdmVKkzR3FXzBqKaamJjRaBhyi1CWXhcouKoKFbfkYUqvFgehui_uhMYL0JTjRcg5t274kB4bXLGfywq670IowQRXZfgKcOgSiwKGxsl9AnJjzraeBv_S6wyRXFX9JM0EXS1g3-uH5Z7Zuf09iHdGPGh-7nhI6A6C8Op3ovCAWd20a1Sa7g4m9oe9-UCpMSjau5Kit0oa8LU3_ESulziDrQljQskwQgVGObw"
    
    # Заголовки с JWT токеном
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Данные сообщения - тестируем исправление таймаута
    data = {
        "conversation": 8,  # ID беседы, которая теперь работает
        "message": "🔧 ТЕСТ ИСПРАВЛЕНИЯ ТАЙМАУТА: Расскажи мне коротко о технологиях будущего (максимум 100 слов)",
        "is_bot": False,
        "message_type": 0
    }
    
    print("🚀 Отправляем тестовое сообщение для проверки исправления таймаута...")
    print(f"📤 URL: {url}")
    print(f"📝 Сообщение: {data['message'][:50]}...")
    print(f"🔑 Токен: {token[:20]}...")
    print(f"💬 Беседа: {data['conversation']}")
    print(f"👤 User sub: eaf55af9-0467-44dc-8f13-8af689b97a09")
    
    try:
        response = requests.post(url, headers=headers, json=data)
        
        print(f"📊 Статус ответа: {response.status_code}")
        
        if response.status_code == 201:
            print("✅ Сообщение успешно отправлено!")
            response_data = response.json()
            print(f"📋 ID сообщения: {response_data.get('id', 'N/A')}")
            print(f"📋 Message ID: {response_data.get('message_id', 'N/A')}")
            print(f"📋 Conversation ID: {response_data.get('conversation_id', 'N/A')}")
            print(f"📋 Сохранено в БД: {response_data.get('created_at', 'N/A')}")
            
            # Проверяем, что сообщение действительно сохранено
            if 'id' in response_data:
                print(f"🎯 Сообщение {response_data['id']} должно быть отправлено в Kafka для обработки LLM")
                print(f"⏳ Ожидаем ответ от LLM... (проверьте логи Kafka Consumer)")
            else:
                print("⚠️ ID сообщения не найден в ответе")
                
        else:
            print(f"❌ Ошибка при отправке: {response.status_code}")
            print(f"📋 Ответ: {response.text}")
            
    except Exception as e:
        print(f"💥 Исключение: {e}")

if __name__ == "__main__":
    send_message()
