"""
Простой тест Kafka подключения
"""
import asyncio
import json
from aiokafka import AIOKafkaConsumer

async def test_kafka():
    # Попробуем разные серверы
    servers = [
        "localhost:9092",
        "host.docker.internal:9092", 
        "kafka:9092",
        "chat-service-kafka:29092"
    ]
    
    for server in servers:
        print(f"\n🧪 Тестируем подключение к: {server}")
        try:
            consumer = AIOKafkaConsumer(
                "chat-ui-messages",
                bootstrap_servers=server,
                group_id="test-consumer-group",
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                consumer_timeout_ms=5000  # 5 секунд таймаут
            )
            
            await consumer.start()
            print(f"✅ Подключение к {server} успешно!")
            
            # Попробуем получить одно сообщение
            print("📡 Ждем сообщения...")
            async for message in consumer:
                print(f"📨 Получено сообщение: {message.value}")
                break
                
            await consumer.stop()
            print(f"✅ Тест для {server} завершен")
            return  # Если успешно, выходим
            
        except Exception as e:
            print(f"❌ Ошибка подключения к {server}: {e}")
            try:
                await consumer.stop()
            except:
                pass
    
    print("❌ Не удалось подключиться ни к одному серверу")

if __name__ == "__main__":
    asyncio.run(test_kafka())
