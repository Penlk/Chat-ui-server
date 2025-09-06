#!/usr/bin/env python3
"""
Тестовый скрипт для проверки streaming запроса к cloud.ru API
"""

import os
import json
import openai

def test_cloud_stream():
    """Тестируем streaming запрос к cloud.ru API"""
    
    # Получаем API ключ из переменных окружения
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("❌ OPENAI_API_KEY не найден в переменных окружения")
        return
    
    # URL для cloud.ru API
    url = "https://foundation-models.api.cloud.ru/v1"
    
    print(f"🔑 API Key: {api_key[:20]}...")
    print(f"🌐 Base URL: {url}")
    
    # Настраиваем OpenAI для старой версии
    openai.api_key = api_key
    openai.api_base = url
    
    print("\n🚀 Отправляем streaming запрос...")
    
    try:
        # Делаем streaming запрос для старой версии openai
        response = openai.ChatCompletion.create(
            model="deepseek-ai/DeepSeek-R1-Distill-Llama-70B",
            max_tokens=1000,
            temperature=0.5,
            presence_penalty=0,
            top_p=0.95,
            stream=True,  # Включаем streaming
            messages=[{
                "role": "user",
                "content": "Привет! Расскажи коротко о том, как работает машинное обучение."
            }]
        )
        
        print("📡 Получаем streaming ответ:")
        print("-" * 50)
        
        full_content = ""
        chunk_count = 0
        
        # Обрабатываем streaming ответ
        for chunk in response:
            chunk_count += 1
            print(f"\n📦 Chunk #{chunk_count}:")
            print(f"   Type: {type(chunk)}")
            print(f"   Chunk object: {chunk}")
            
            # Проверяем структуру chunk
            if hasattr(chunk, 'choices') and chunk.choices:
                choice = chunk.choices[0]
                print(f"   Choice: {choice}")
                
                if hasattr(choice, 'delta') and choice.delta:
                    delta = choice.delta
                    print(f"   Delta: {delta}")
                    
                    # Проверяем content (стандартный OpenAI)
                    if hasattr(delta, 'content') and delta.content:
                        content = delta.content
                        print(f"   Content: '{content}'")
                        full_content += content
                        print(f"   📝 Текущий текст: '{full_content}'")
                    
                    # Проверяем reasoning_content (DeepSeek модель)
                    if hasattr(delta, 'reasoning_content') and delta.reasoning_content:
                        reasoning_content = delta.reasoning_content
                        print(f"   Reasoning Content: '{reasoning_content}'")
                        full_content += reasoning_content
                        print(f"   📝 Текущий текст: '{full_content}'")
                
                # Проверяем finish_reason
                if hasattr(choice, 'finish_reason') and choice.finish_reason:
                    print(f"   🏁 Finish reason: {choice.finish_reason}")
            
            # Проверяем другие атрибуты chunk
            print(f"   All attributes: {dir(chunk)}")
            
            # Ограничиваем количество chunk'ов для тестирования
            if chunk_count >= 10:
                print("\n⏹️ Останавливаем после 10 chunk'ов для тестирования")
                break
        
        print("\n" + "=" * 50)
        print("📋 ИТОГОВЫЙ РЕЗУЛЬТАТ:")
        print(f"📊 Всего chunk'ов: {chunk_count}")
        print(f"📝 Полный текст: '{full_content}'")
        print(f"📏 Длина текста: {len(full_content)} символов")
        
    except Exception as e:
        print(f"❌ Ошибка при запросе: {e}")
        print(f"   Тип ошибки: {type(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_cloud_stream()
