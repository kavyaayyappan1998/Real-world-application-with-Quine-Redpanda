from kafka import KafkaProducer, KafkaConsumer
from cassandra.cluster import Cluster
import json
import time
import asyncio
import requests

redpanda_host = 'localhost'
redpanda_port = 9092
follower_data_topic = 'follower_data'
follower_graph_topic = 'follower_graph'
cassandra_host = 'localhost'
cassandra_port = 9042
quine_host = 'http://localhost:8080'
cypher_script_path = './follower_graph.cypher'

# Recipe 1: transform incoming data into graph nodes
recipe_1 = '''
MATCH (n)
WHERE n.user_id IS NOT NULL AND n.follower_id IS NOT NULL
RETURN n
'''

# Recipe 2: flag influential users
recipe_2 = '''
MATCH (u:User)
RETURN u
'''

cluster = Cluster([cassandra_host])
session = cluster.connect('social_graph')

producer = KafkaProducer(
    bootstrap_servers=f'{redpanda_host}:{redpanda_port}',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    follower_data_topic,
    bootstrap_servers=f'{redpanda_host}:{redpanda_port}',
    auto_offset_reset='earliest',
    group_id='follower-processor',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

def cypher_literal(value):
    return json.dumps(value)

def send_to_quine(query, parameters=None):
    if parameters is None:
        parameters = {}

    try:
        # Inline the parameters into the Cypher query
        for key, value in parameters.items():
            query = query.replace(f'${key}', cypher_literal(value))

        response = requests.post(
            f'{quine_host}/api/v1/query/cypher',
            json={'text': query},
            headers={'Content-Type': 'application/json'}
        )

        if response.status_code == 200:
            try:
                return response.json()
            except Exception:
                return response.text

        print(f'Quine error {response.status_code}: {response.text}')
        return None

    except Exception as e:
        print(f'Quine request failed: {e}')
        return None

def save_to_cassandra(user_id, followers):
    session.execute(
        'UPDATE follower_graph SET followers = followers + %s WHERE user_id = %s',
        (followers, user_id)
    )
    print(f'Cassandra updated: {user_id} <- {followers}')

def process_follower_data(data):
    user_id = data['user_id']
    followers = data['followers']

    with open(cypher_script_path, 'r') as f:
        query = f.read()

    send_to_quine(query, {'userId': user_id, 'followers': followers})

    return {
        'user_id': user_id,
        'followers': followers,
        'processed_at': int(time.time())
    }

async def process_data(message):
    data = message.value

    # Apply Recipe 1 for data transformation
    transformed_data = send_to_quine(recipe_1, data)

    # Apply Recipe 2 for core processing logic
    processed_data = send_to_quine(recipe_2, transformed_data or {})

    # Store in Cassandra
    save_to_cassandra(data['user_id'], data['followers'])

    # Run CypherQL script loaded from disk
    follower_graph = process_follower_data(data)

    # Send to output topic
    producer.send(follower_graph_topic, follower_graph)
    producer.flush()

    print(f'Published to {follower_graph_topic}: {follower_graph}')

async def main():
    print('Processor started. Waiting for messages...')
    for message in consumer:
        await process_data(message)

if __name__ == '__main__':
    asyncio.run(main())