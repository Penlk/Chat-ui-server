#!/usr/bin/env python
import requests
import json
import os

def test_cloud_api():
    """Тестируем API cloud.ru напрямую"""
    
    # Настройки API
    api_key = "insert_the_real_key_here"
    api_base = "https://foundation-models.api.cloud.ru"
    
    url = f"{api_base}/v1/chat/completions"
    
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    data = {
        "model": "deepseek-ai/DeepSeek-R1-Distill-Llama-70B",
        "messages": [
            {
                "role": "user",
                "content": "Hello! Tell me about the weather in Moscow"
            }
        ],
        "temperature": 0.7,
        "top_p": 0.9,
        "max_tokens": 300,
        "frequency_penalty": 0.5,
        "presence_penalty": 0.3,
        "stream": True  # Включаем stream
    }
    
    print("🚀 Тестируем API cloud.ru...")
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
                        if data_str.strip() == '[DONE]':
                            print("✅ Stream завершен")
                            break
                        try:
                            data_json = json.loads(data_str)
                            if 'choices' in data_json and len(data_json['choices']) > 0:
                                choice = data_json['choices'][0]
                                if 'delta' in choice and 'content' in choice['delta']:
                                    content = choice['delta']['content']
                                    print(f"💬 Контент: {content}")
                        except json.JSONDecodeError:
                            print(f"❌ Не удалось распарсить JSON: {data_str}")
        else:
            print(f"❌ Ошибка: {response.text}")
            
    except Exception as e:
        print(f"❌ Исключение: {e}")

if __name__ == "__main__":
    test_cloud_api()
