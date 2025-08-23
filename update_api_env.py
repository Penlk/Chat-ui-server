#!/usr/bin/env python
import os

def update_api_env():
    """Обновляем переменные окружения для API cloud.ru"""
    
    # API ключ для cloud.ru
    api_key = "insert_the_real_key_here"
    
    # Base URL для cloud.ru
    api_base = "https://foundation-models.api.cloud.ru"
    
    # Устанавливаем переменные окружения
    os.environ['OPENAI_API_KEY'] = api_key
    os.environ['OPENAI_API_PROXY'] = api_base
    
    print(f"✅ API ключ установлен: {api_key[:20]}...")
    print(f"✅ API Base URL установлен: {api_base}")
    
    # Проверяем настройки
    print("\n📋 Текущие настройки:")
    print(f"API Key: {os.getenv('OPENAI_API_KEY', 'Не установлен')[:20]}...")
    print(f"API Base: {os.getenv('OPENAI_API_PROXY', 'Не установлен')}")

if __name__ == "__main__":
    update_api_env()
