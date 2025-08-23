#!/usr/bin/env python
import os
import sys
import django

# Настройка Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'chatgpt_ui_server.settings')
django.setup()

from chat.models import Setting

def update_api_settings():
    """Обновляем настройки API для cloud.ru"""
    
    # API ключ для cloud.ru
    api_key = "insert_the_real_key_here"
    
    # Base URL для cloud.ru
    api_base = "https://foundation-models.api.cloud.ru"
    
    # Обновляем или создаем настройку API ключа
    setting, created = Setting.objects.get_or_create(
        name='openai_api_key',
        defaults={'value': api_key}
    )
    if not created:
        setting.value = api_key
        setting.save()
    
    print(f"✅ API ключ обновлен: {api_key[:20]}...")
    
    # Устанавливаем переменную окружения для API base
    os.environ['OPENAI_API_PROXY'] = api_base
    print(f"✅ API Base URL установлен: {api_base}")
    
    # Проверяем настройки
    print("\n📋 Текущие настройки:")
    print(f"API Key: {Setting.objects.get(name='openai_api_key').value[:20]}...")
    print(f"API Base: {os.getenv('OPENAI_API_PROXY', 'Не установлен')}")

if __name__ == "__main__":
    update_api_settings()
