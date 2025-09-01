#!/usr/bin/env python3
"""
Скрипт для отправки сообщения напрямую в Kafka
"""
import asyncio
import json
from aiokafka import AIOKafkaProducer

async def send_kafka_message():
    """Отправляет сообщение в Kafka"""
    
    # Конфигурация Kafka
    bootstrap_servers = "localhost:9092"
    topic = "chat-ui-messages"
    
    # Тестовое сообщение
    message_data = {
        "message_id": "test-message-001",
        "request_id": "test-request-001",
        "timestamp": "2025-09-01T19:00:00.000000Z",
        "payload": {
            "conversation_id": 1,
            "message": "сколько железа в яблоке",
            "is_bot": False,
            "message_type": 0,
            "saved_message_id": 279,
            "message_id": 36,
            "user_context": {
                "email": "test2@example.com",
                "full_name": "Test2 Account",
                "active_org_id": None,
                "org_role": "user",
                "is_org_owner": False
            },
            "request_metadata": {
                "source_ip": "127.0.0.1",
                "user_agent": "Python/3.10"
            }
        },
        "operation": "start_stream"
    }
    
    print(f"📤 Sending message to Kafka topic: {topic}")
    print(f"📤 Message: {json.dumps(message_data, indent=2)}")
    
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        await producer.start()
        
        # Отправляем сообщение
        await producer.send_and_wait(topic, message_data)
        
        print("✅ Message sent successfully to Kafka!")
        
        await producer.stop()
        
    except Exception as e:
        print(f"❌ Error sending message to Kafka: {e}")

if __name__ == "__main__":
    asyncio.run(send_kafka_message())
