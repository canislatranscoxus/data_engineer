import json
import uuid
from kafka import KafkaConsumer

# Generates a brand new unique group ID on every run so 'earliest' always triggers
unique_group_id = f"ninja-group-{uuid.uuid4()}"

consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=unique_group_id,
)

print(f"⚡ Ninja Kafka Consumer Active (Group: {unique_group_id}). Waiting for messages...")

for message in consumer:
    try:
        payload = json.loads(message.value.decode('utf-8'))
        print(f"📥 Received [Partition: {message.partition}, Offset: {message.offset}] -> {payload}", flush=True)
    except Exception as e:
        print(f"⚠️ Raw Message: {message.value.decode('utf-8')} | Error: {e}", flush=True)