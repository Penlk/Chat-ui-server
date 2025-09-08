#!/usr/bin/env python
import requests
import json
import datetime

def test_and_save_response():
    print("🚀 Тестируем полный ответ от LLM")
    
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
        "max_tokens": 300,  # Увеличили лимит для полного ответа
        "frequency_penalty": 0.5,
        "presence_penalty": 0.3
    }
    
    print(f"📤 Вопрос: {data['message'][0]['content']}")
    print(f"📤 URL: {url}")
    
    try:
        response = requests.post(url, headers=headers, json=data, stream=True, timeout=60)
        
        print(f"📡 Статус: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Собираем полный ответ...")
            
            full_response = ""
            response_parts = []
            
            for line in response.iter_lines():
                if line:
                    line_str = line.decode('utf-8')
                    
                    if line_str.startswith('data: '):
                        data_str = line_str[6:]
                        try:
                            data_json = json.loads(data_str)
                            if 'content' in data_json:
                                content = data_json['content']
                                full_response += content
                                response_parts.append(content)
                                print(f"📝 {content}", end="", flush=True)
                        except json.JSONDecodeError:
                            pass
                    
                    if 'done' in line_str:
                        print("\n✅ Ответ завершен")
                        break
            
            # Сохраняем в файл
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"llm_response_{timestamp}.txt"
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("=== ТЕСТ ОТВЕТА ОТ LLM ===\n")
                f.write(f"Время: {datetime.datetime.now()}\n")
                f.write(f"Модель: {data['name']}\n")
                f.write(f"Вопрос: {data['message'][0]['content']}\n")
                f.write("=" * 50 + "\n\n")
                f.write("ПОЛНЫЙ ОТВЕТ:\n")
                f.write(full_response)
                f.write("\n\n" + "=" * 50 + "\n")
                f.write("ЧАСТИ ОТВЕТА (для отладки):\n")
                for i, part in enumerate(response_parts, 1):
                    f.write(f"{i:3d}: '{part}'\n")
            
            print(f"\n📁 Ответ сохранен в файл: {filename}")
            print(f"📊 Всего частей: {len(response_parts)}")
            print(f"📊 Длина ответа: {len(full_response)} символов")
            
            return full_response
            
        else:
            print(f"❌ Ошибка HTTP {response.status_code}")
            print(f"❌ Ответ: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Исключение: {e}")
        return None

if __name__ == "__main__":
    result = test_and_save_response()
    if result:
        print(f"\n✅ Тест завершен успешно!")
    else:
        print(f"\n❌ Тест завершился с ошибкой!")
