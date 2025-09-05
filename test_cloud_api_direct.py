#!/usr/bin/env python3
"""
Прямой тест Cloud.ru API для проверки формата SSE ответа
"""
import asyncio
import aiohttp
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_cloud_api():
    """Тестируем Cloud.ru API напрямую"""
    url = "https://foundation-models.api.cloud.ru/v1/chat/completions"
    api_key = "YzU1MGExYWYtODhiYy00MWE1LWIzMjAtMWMxMGYxN2IzZGVh.bbf892ff8b0054533d307ed3ba2ffae5"
    
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    data = {
        "model": "deepseek-ai/DeepSeek-R1-Distill-Llama-70B",
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant."
            },
            {
                "role": "user",
                "content": "Скажи коротко: привет!"
            }
        ],
        "temperature": 0.7,
        "top_p": 0.9,
        "max_tokens": 2000,
        "frequency_penalty": 0.5,
        "presence_penalty": 0.3,
        "stream": True
    }
    
    logger.info(f"🌐 Making request to Cloud.ru API...")
    logger.info(f"📤 Data: {data}")
    
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(url, json=data, headers=headers) as response:
            if response.status == 200:
                logger.info(f"✅ Received response from Cloud.ru API")
                
                line_count = 0
                async for line in response.content:
                    line_str = line.decode('utf-8').strip()
                    if line_str:
                        line_count += 1
                        logger.info(f"📋 Line {line_count}: {line_str}")
                        
                        # Проверяем разные варианты завершения
                        if ("event: done" in line_str or 
                            "[DONE]" in line_str or 
                            '"finish_reason":"stop"' in line_str or
                            '"finish_reason":"length"' in line_str):
                            logger.info(f"✅ Found completion signal: {line_str}")
                            break
                        
                        # Ограничиваем количество строк для теста
                        if line_count > 20:
                            logger.info(f"🛑 Stopping after {line_count} lines for test")
                            break
            else:
                error_text = await response.text()
                logger.error(f"❌ API error: {response.status} - {error_text}")

if __name__ == "__main__":
    asyncio.run(test_cloud_api())
