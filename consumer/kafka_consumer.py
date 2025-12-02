from kafka import KafkaConsumer
from minio import Minio

from datetime import datetime
import json
from dotenv import load_dotenv
import os
import io

load_dotenv()

TOPIC = "social_stream"
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

def connect_to_kafka():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers = bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="comment-consumer-1",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    print("Connected to kafka")

    return consumer

def connect_to_minio():
    client = Minio(
        "minio:9000",
        access_key = os.getenv("MINIO_ROOT_USER"),
        secret_key = os.getenv("MINIO_ROOT_PASSWORD"),
        secure = False
    )
    print("Connected to minio")
    return client

def write_to_minio(bucket_name: str, data, client):

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Minio bucket {bucket_name} created")
    else:
        print(f"Minio bucket {bucket_name} already exists")
    
    if not data:
        print("No data to write")

    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
    filename = f'youtube_comments_{timestamp}.json'

    json_bytes = json.dumps(data).encode('utf-8')
    data_stream = io.BytesIO(json_bytes)
    client.put_object(
        bucket_name,
        filename,
        data_stream,
        len(json_bytes)
    )
    print(f"Data written to Minio at {timestamp}")

if __name__ == "__main__":
    try:
        print("Connecting to Kafka...")
        consumer = connect_to_kafka()
        print("Consumer connected.")

        client = connect_to_minio()
        print("MinIO connected.")

        buffer = []
        BATCH_SIZE = 30

        print("Entering consumer loop...")
        print(consumer)
        for message in consumer:
            print("MESSAGE:", message.value)
            buffer.append(message.value)

            if len(buffer) >= BATCH_SIZE:
                write_to_minio("youtube-comments", buffer, client)
                buffer = []

    except Exception as e:
        print("ERROR IN CONSUMER:", e) 
