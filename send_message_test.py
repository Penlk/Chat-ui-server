#!/usr/bin/env python3
"""
Тестовый скрипт для отправки сообщения на Gateway
"""
import requests
import json

def send_message():
    """Отправляет тестовое сообщение на Gateway"""
    
    url = "http://localhost:8002/api/chat/messages/"
    
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJVaGtKejZ2TlREWEpnQnoxdGJjSXhjLVc2U0V2NERpQVZVOHFhTGZrckZZIn0.eyJleHAiOjE3NTY3Mjc2NjYsImlhdCI6MTc1NjcyNzM2NiwianRpIjoib25ydHJvOmU0ZmRjYjgxLWE3ZDgtY2MxYS02OWQxLTE0Yjg0MzRlYTk3OCIsImlzcyI6Imh0dHA6Ly9rZXljbG9hazo4MDgwL3JlYWxtcy9hdXRoLXNlcnZpY2UiLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiZWFmNTVhZjktMDQ2Ny00NGRjLThmMTMtOGFmNjg5Yjk3YTA5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYXV0aC1zZXJ2aWNlIiwic2lkIjoiZDI0YmM3YmYtMmRlMC00ZDc2LWE3MGUtZjRkYjFlOGI0ZjZmIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIgaHR0cDovLzEyNy4wLjAuMTo4MDA5Il0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLWF1dGgtc2VydmljZSIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwibmFtZSI6IlRlc3QyIEFjY291bnQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0ZXN0MkBleGFtcGxlLmNvbSIsImdpdmVuX25hbWUiOiJUZXN0MiIsImZhbWlseV9uYW1lIjoiQWNjb3VudCIsImVtYWlsIjoidGVzdDJAZXhhbXBsZS5jb20ifQ.m6o9geUGyW8WRYqsJc8Yb5VHTjyiCUiVJROox2wMlEyJ6EhbABFCe_UVBLXK8FszxQtjVQGsfAlocK3unNbuTD9X8OD4HDJLZvtfBkkqiyLWDN88cbUdmVKkzR3FXzBqKaamJjRaBhyi1CWXhcouKoKFbfkYUqvFgehui_uhMYL0JTjRcg5t274kB4bXLGfywq670IowQRXZfgKcOgSiwKGxsl9AnJjzraeBv_S6wyRXFX9JM0EXS1g3-uH5Z7Zuf09iHdGPGh-7nhI6A6C8Op3ovCAWd20a1Sa7g4m9oe9-UCpMSjau5Kit0oa8LU3_ESulziDrQljQskwQgVGObw"
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    # Данные сообщения
    data = {
        "conversation": 1,  # ID существующей беседы
        "message": "Тестируем упрощенную версию: Kafka consumer только передает SSE поток на фронтенд! Расскажи мне что-нибудь интересное о космосе",
        "is_bot": False,
        "message_type": 0
    }
    
    print(f"🌐 Sending request to {url}")
    print(f"📤 Headers: {headers}")
    print(f"📤 Data: {json.dumps(data, indent=2)}")
    
    try:
        response = requests.post(url, headers=headers, json=data)
        
        print(f"📥 Response status: {response.status_code}")
        print(f"📥 Response headers: {dict(response.headers)}")
        
        if response.status_code == 200 or response.status_code == 201:
            print(f"✅ Success! Response: {response.text}")
        else:
            print(f"❌ Error: {response.text}")
            
    except Exception as e:
        print(f"💥 Exception: {e}")

if __name__ == "__main__":
    send_message()
