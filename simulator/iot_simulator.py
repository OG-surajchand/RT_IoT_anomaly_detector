import time
import json
import random
from kafka import KafkaProducer

KAFKA_TOPIC = 'iot_data'
KAFKA_SERVER = 'kafka:9092'

DEVICES = ['sensor-A', 'sensor-B', 'sensor-C']

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

print("Connecting to Kafka...")
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=json_serializer,
    api_version=(0, 10, 1)
)
print("Connected to Kafka!")

if __name__ == '__main__':
    while True:
        try:
            device_id = random.choice(DEVICES)
            if random.random() < 0.9:
                temperature = round(random.uniform(20.0, 30.0), 2)
            else:
                temperature = round(random.uniform(100.0, 120.0), 2)

            data = {
                'device_id': device_id,
                'timestamp': int(time.time()),
                'temperature': temperature,
                'is_anomaly': random.random() < 0.05,
                'user': "Suraj Chand"
            }

            print(f"Sending: {data}")
            producer.send(KAFKA_TOPIC, data)
            
            time.sleep(0.5)

        except Exception as e:
            print(f"Error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)