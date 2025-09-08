#!/usr/bin/env python3
"""
Скрипт для создания новой тестовой беседы
"""
import requests
import json

def create_conversation():
    # URL для создания беседы
    url = "http://localhost:8003/test-conversations/"
    
    # JWT токен
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJVaGtKejZ2TlREWEpnQnoxdGJjSXhjLVc2U0V2NERpQVZVOHFhTGZrckZZIn0.eyJleHAiOjE3NTY3Mjc2NjYsImlhdCI6MTc1NjcyNzM2NiwianRpIjoib25ydHJvOmU0ZmRjYjgxLWE3ZDgtY2MxYS02OWQxLTE0Yjg0MzRlYTk3OCIsImlzcyI6Imh0dHA6Ly9rZXljbG9hazo4MDgwL3JlYWxtcy9hdXRoLXNlcnZpY2UiLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiZWFmNTVhZjktMDQ2Ny00NGRjLThmMTMtOGFmNjg5Yjk3YTA5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYXV0aC1zZXJ2aWNlIiwic2lkIjoiZDI0YmM3YmYtMmRlMC00ZDc2LWE3MGUtZjRkYjFlOGI0ZjZmIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIgaHR0cDovLzEyNy4wLjAuMTo4MDA5Il0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLWF1dGgtc2VydmljZSIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlYWx0X2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwibmFtZSI6IlRlc3QyIEFjY291bnQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0ZXN0MkBleGFtcGxlLmNvbSIsImdpdmVuX25hbWUiOiJUZXN0MiIsImZhbWlseV9uYW1lIjoiQWNjb3VudCIsImVtYWlsIjoidGVzdDJAZXhhbXBsZS5jb20ifQ.m6o9geUGyW8WRYqsJc8Yb5VHTjyiCUiVJROox2wMlEyJ6EhbABFCe_UVBLXK8FszxQtjVQGsfAlocK3unNbuTD9X8OD4HDJLZvtfBkkqiyLWDN88cbUdmVKkzR3FXzBqKaamJjRaBhyi1CWXhcouKoKFbfkYUqvFgehui_uhMYL0JTjRcg5t274kB4bXLGfywq670IowQRXZfgKcOgSiwKGxsl9AnJjzraeBv_S6wyRXFX9JM0EXS1g3-uH5Z7Zuf09iHdGPGh-7nhI6A6C8Op3ovCAWd20a1Sa7g4m9oe9-UCpMSjau5Kit0oa8LU3_ESulziDrQljQskwQgVGObw"
    
    # Заголовки с JWT токеном
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Данные для создания беседы
    data = {
        "topic": "🚀 ТЕСТ ПРЯМОГО API - Новая беседа"
    }
    
    print("🚀 Создаем новую тестовую беседу...")
    print(f"📤 URL: {url}")
    print(f"📝 Topic: {data['topic']}")
    print(f"🔑 Токен: {token[:20]}...")
    print(f"👤 User sub: eaf55af9-0467-44dc-8f13-8af689b97a09")
    
    try:
        response = requests.post(url, headers=headers, json=data)
        
        print(f"📊 Статус ответа: {response.status_code}")
        
        if response.status_code == 201:
            print("✅ Беседа создана успешно!")
            conversation_data = response.json()
            print(f"📋 ID беседы: {conversation_data.get('id', 'N/A')}")
            print(f"📋 Topic: {conversation_data.get('topic', 'N/A')}")
            print(f"📋 Created: {conversation_data.get('created_at', 'N/A')}")
            
            # Теперь попробуем отправить сообщение в эту беседу
            print(f"\n📝 Теперь попробуем отправить сообщение в беседу {conversation_data.get('id')}...")
            await send_message_to_conversation(conversation_data.get('id'))
            
        else:
            print(f"❌ Ошибка при создании беседы: {response.status_code}")
            print(f"📋 Ответ: {response.text}")
            
    except Exception as e:
        print(f"💥 Исключение: {e}")

def send_message_to_conversation(conversation_id):
    """Отправка сообщения в указанную беседу"""
    # URL для отправки сообщения
    url = "http://localhost:8003/api/chat/messages/"
    
    # JWT токен
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJVaGtKejZ2TlREWEpnQnoxdGJjSXhjLVc2U0V2NERpQVZVOHFhTGZrckZZIn0.eyJleHAiOjE3NTY3Mjc2NjYsImlhdCI6MTc1NjcyNzM2NiwianRpIjoib25ydHJvOmU0ZmRjYjgxLWE3ZDgtY2MxYS02OWQxLTE0Yjg0MzRlYTk3OCIsImlzcyI6Imh0dHA6Ly9rZXljbG9hazo4MDgwL3JlYWxtcy9hdXRoLXNlcnZpY2UiLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiZWFmNTVhZjktMDQ2Ny00NGRjLThmMTMtOGFmNjg5Yjk3YTA5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYXV0aC1zZXJ2aWNlIiwic2lkIjoiZDI0YmM3YmYtMmRlMC00ZDc2LWE3MGUtZjRkYjFlOGI0ZjZmIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIgaHR0cDovLzEyNy4wLjAuMTo4MDA5Il0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLWF1dGgtc2VydmljZSIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlYWx0X2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwibmFtZSI6IlRlc3QyIEFjY291bnQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0ZXN0MkBleGFtcGxlLmNvbSIsImdpdmVuX25hbWUiOiJUZXN0MiIsImZhbWlseV9uYW1lIjoiQWNjb3VudCIsImVtYWlsIjoidGVzdDJAZXhhbXBsZS5jb20ifQ.m6o9geUGyW8WRYqsJc8Yb5VHTjyiCUiVJROox2W8WRYqsJc8Yb5VHTjyiCUiVJROox2wMlEyJ6EhbABFCe_UVBLXK8FszxQtjVQGsfAlocK3unNbuTD9X8OD4HDJLZvtfBkkqiyLWDN88cbUdmVKkzR3FXzBqKaamJjRaBhyi1CWXhcouKoKFbfkYUqvFgehui_uhMYL0JTjRcg5t274kB4bXLGfywq670IowQRXZfgKcOgSiwKGxsl9AnJjzraeBv_S6wyRXFX9JM0EXS1g3-uH5Z7Zuf09iHdGPGh-7nhI6A6C8Op3ovCAWd20a1Sa7g4m9oe9-UCpMSjau5Kit0oa8LU3_ESulziDrQljQskwQgVGObw"
    
    # Заголовки с JWT токеном
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Данные сообщения
    data = {
        "conversation": conversation_id,  # Используем ID созданной беседы
        "message": "🚀 ТЕСТ ПРЯМОГО API: Расскажи мне коротко о технологиях будущего (максимум 100 слов)",
        "is_bot": False,
        "message_type": 0
    }
    
    print(f"📤 Отправляем сообщение в беседу {conversation_id}...")
    
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
    create_conversation()
