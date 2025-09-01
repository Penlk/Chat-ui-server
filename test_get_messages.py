import requests
import json

# Тест GET запроса для получения сообщений
url = "http://localhost:8003/api/chat/messages/"
headers = {
    "X-User-Data": '{"jwt_token":"eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJVaGtKejZ2TlREWEpnQnoxdGJjSXhjLVc2U0V2NERpQVZVOHFhTGZrckZZIn0.eyJleHAiOjE3NTYxMzkxOTAsImlhdCI6MTc1NjEzODg5MCwianRpIjoib25ydHJvOjRiYTc2NTIwLTQ3NjAtMTk4NC02YWNmLTNlNDA3MDIyZjE3ZiIsImlzcyI6Imh0dHA6Ly9rZXljbG9hazo4MDgwL3JlYWxtcy9hdXRoLXNlcnZpY2UiLCJhdWQiOiJhY2NvdW50Iiwic3ViIjoiZWFmNTVhZjktMDQ2Ny00NGRjLThmMTMtOGFmNjg5Yjk3YTA5IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYXV0aC1zZXJ2aWNlIiwic2lkIjoiOTI3YTNjZDctMGNiMy00MGUyLWIwYzUtYTkxNzQ4MWJiZGZiIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIgaHR0cDovLzEyNy4wLjAuMTo4MDk5Il0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJkZWZhdWx0LXJvbGVzLWF1dGgtc2VydmljZSIsIm9mZmxpbmVfYWNjZXNzIiwidW1hX2F1dGhvcml6YXRpb24iXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJzY29wZSI6InByb2ZpbGUgZW1haWwiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwibmFtZSI6IlRlc3QyIEFjY291bnQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ0ZXN0MkBleGFtcGxlLmNvbSIsImdpdmVuX25hbWUiOiJUZXN0MiIsImZhbWlseV9uYW1lIjoiQWNjb3VudCIsImVtYWlsIjoidGVzdDJAZXhhbXBsZS5jb20ifQ.Jp2kMjEYpjCuHidZdqSdDgtjRiEjGQ4tGx8TJO3mwqi5mv-rHPePFek3N3oF1w5YQrD2YSReU4I7VwRqkqs7M8W1UX62PAje8vh4HT6qS0zlhAPKhOeCsd8t8J3yn6y9pBX8hPyD2F9JUwIsF0nAKTm1aykCuvBkoBse7OfHu5npBEUWnERc6rWhH83pbx7vk-aDYuo2ylH32Ro0tZEjAGUknl2VfigrmJGSncoS-isC6hcSFi5iF7tUBbnffayEuPnHS4o9HdSMB4-6naERIOKZgHhwULjXtLUDK2M2YBSw9azy-ZNeYZVQlyiC5NWR267uUDyOpH4qr-k9EJeL8w","user_data":{"sub":"eaf55af9-0467-44dc-8f13-8af689b97a09","email":"test2@example.com","full_name":"Test2 Account","orgs":[],"active_org_id":null}}'
}

try:
    print("Получаем список сообщений...")
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    
    response = requests.get(url, headers=headers, timeout=30)
    
    print(f"\nСтатус: {response.status_code}")
    print(f"Заголовки ответа: {dict(response.headers)}")
    print(f"Тип контента: {response.headers.get('Content-Type', 'Unknown')}")
    
    if response.status_code == 200:
        print("✅ Сообщения получены!")
        messages = response.json()
        print(f"Количество сообщений: {len(messages)}")
        for i, msg in enumerate(messages[:5]):  # Показываем первые 5 сообщений
            print(f"Сообщение {i+1}:")
            print(f"  ID: {msg.get('id')}")
            print(f"  message_id: {msg.get('message_id')}")
            print(f"  message: {msg.get('message')[:50]}...")
            print(f"  conversation_id: {msg.get('conversation_id')}")
            print()
    else:
        print(f"❌ Ошибка: {response.text}")
        
except Exception as e:
    print(f"❌ Исключение: {e}")
    import traceback
    traceback.print_exc()
