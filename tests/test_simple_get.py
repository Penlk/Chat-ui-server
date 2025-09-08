import requests

# Простой GET тест
url = "http://localhost:8003/api/chat/conversations/"
headers = {
    "X-User-Data": '{"jwt_token":"test","user_data":{"sub":"eaf55af9-0467-44dc-8f13-8af689b97a09"}}'
}

try:
    print("Отправляем GET запрос...")
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    
    response = requests.get(url, headers=headers, timeout=30)
    
    print(f"\nСтатус: {response.status_code}")
    print(f"Заголовки ответа: {dict(response.headers)}")
    print(f"Тип контента: {response.headers.get('Content-Type', 'Unknown')}")
    
    if response.status_code == 200:
        print("✅ Успешно!")
        print(f"Ответ: {response.text[:500]}...")
    else:
        print(f"❌ Ошибка: {response.text}")
        
except Exception as e:
    print(f"❌ Исключение: {e}")
    import traceback
    traceback.print_exc()
