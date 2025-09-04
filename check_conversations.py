#!/usr/bin/env python3
"""
Скрипт для проверки существующих бесед пользователя
"""
import requests
import json

def check_conversations():
    # URL для проверки бесед
    url = "http://localhost:8003/test-conversations/"
    
    # JWT токен
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJVaGtKejZ2TlREWEpnQnoxdGJjSXhjLVc2U0V2NERpQVZVOHFhTGZrckZZIn0.eyJleHAiOjE3NTY3Mjc2NjYsImlhdCI6MTc1NjcyNzM2NiwianRpIjoib25ydHJvOmU0ZmRjYjgxLWE3ZDgtY2MxYS02OWQxLTE0Yjg0MzRlYTk3OCIsImlzcyI6Imh0dHA6Ly9rZXljbG9hazo4MDgwL3JlYWxtcy9hdXRoLXNlcnZpY2UiLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiZWFmNTVhZjktMDQ2Ny00NGRjLThmMTMtOGFmNjg5Yjk3YTA5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYXV0aC1zZXJ2aWNlIiwic2lkIjoiZDI0YmM3YmYtMmRlMC00ZDc2LWE3MGUtZjRkYjFlOGI0ZjZmIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIgaHR0cDovLzEyNy4wLjAuMTo4MDA5Il0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLWF1dGgtc2VydmljZSIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlYWx0X2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwibmFtZSI6IlRlc3QyIEFjY291bnQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0ZXN0MkBleGFtcGxlLmNvbSIsImdpdmVuX25hbWUiOiJUZXN0MiIsImZhbWlseV9uYW1lIjoiQWNjb3VudCIsImVtYWlsIjoidGVzdDJAZXhhbXBsZS5jb20ifQ.m6o9geUGyW8WRYqsJc8Yb5VHTjyiCUiVJROox2wMlEyJ6EhbABFCe_UVBLXK8FszxQtjVQGsfAlocK3unNbuTD9X8OD4HDJLZvtfBkkqiyLWDN88cbUdmVKkzR3FXzBqKaamJjRaBhyi1CWXhcouKoKFbfkYUqvFgehui_uhMYL0JTjRcg5t274kB4bXLGfywq670IowQRXZfgKcOgSiwKGxsl9AnJjzraeBv_S6wyRXFX9JM0EXS1g3-uH5Z7Zuf09iHdGPGh-7nhI6A6C8Op3ovCAWd20a1Sa7g4m9oe9-UCpMSjau5Kit0oa8LU3_ESulziDrQljQskwQgVGObw"
    
    # Заголовки с JWT токеном
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    print("🔍 Проверяем существующие беседы пользователя...")
    print(f"📤 URL: {url}")
    print(f"🔑 Токен: {token[:20]}...")
    print(f"👤 User sub: eaf55af9-0467-44dc-8f13-8af689b97a09")
    
    try:
        response = requests.get(url, headers=headers)
        
        print(f"📊 Статус ответа: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Беседы получены успешно!")
            conversations = response.json()
            
            if conversations:
                print(f"📋 Найдено бесед: {len(conversations)}")
                for i, conv in enumerate(conversations):
                    print(f"  {i+1}. ID: {conv.get('id')}, Topic: {conv.get('topic')}, Created: {conv.get('created_at')}")
            else:
                print("📋 Беседы не найдены")
                
        else:
            print(f"❌ Ошибка при получении бесед: {response.status_code}")
            print(f"📋 Ответ: {response.text}")
            
    except Exception as e:
        print(f"💥 Исключение: {e}")

if __name__ == "__main__":
    check_conversations()
