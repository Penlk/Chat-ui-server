#!/usr/bin/env python3
"""
Тест для проверки api/conversation_cloud эндпоинта
"""
import asyncio
import aiohttp
import json

async def test_conversation_cloud():
    """Тестирует api/conversation_cloud эндпоинт"""
    
    url = "http://localhost:8003/api/conversation_cloud/"
    
    # Тестовые данные
    test_data = {
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
        "presence_penalty": 0.3,
        "openaiApiKey": "YzU1MGExYWYtODhiYy00MWE1LWIzMjAtMWMxMGYxN2IzZGVh.bbf892ff8b0054533d307ed3ba2ffae5"
    }
    
    headers = {
        'Content-Type': 'application/json'
    }
    
    print(f"🌐 Testing {url}")
    print(f"📤 Sending data: {json.dumps(test_data, indent=2)}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=test_data, headers=headers) as response:
                print(f"📥 Response status: {response.status}")
                print(f"📥 Response headers: {dict(response.headers)}")
                
                if response.status == 200:
                    print("✅ Success! Reading response...")
                    response_text = await response.text()
                    print(f"📡 Response: {response_text}")
                else:
                    error_text = await response.text()
                    print(f"❌ Error: {error_text}")
                    
    except Exception as e:
        print(f"💥 Exception: {e}")

if __name__ == "__main__":
    asyncio.run(test_conversation_cloud())
