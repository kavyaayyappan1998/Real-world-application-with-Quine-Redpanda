from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

users = [f'user_{i}' for i in range(1, 20)]

def generate_follower_event():
    user = random.choice(users)
    follower = random.choice([u for u in users if u != user])
    return {
        'user_id': user,
        'followers': [follower],
        'timestamp': int(time.time())
    }

for i in range(50):
    event = generate_follower_event()
    producer.send('follower_data', event)
    producer.flush()
    print(f"Sent: {event}")
    time.sleep(0.5)