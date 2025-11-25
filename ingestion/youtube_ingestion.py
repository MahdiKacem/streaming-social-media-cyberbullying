from kafka import KafkaProducer
from dotenv import load_dotenv

import os
import json

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_ID = "UC2HNqPgeKtVr2gZmLeeIk3g"
TOPIC = "social_stream"
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')


producer = KafkaProducer(
            bootstrap_servers = bootstrap_servers,
            value_serializer = lambda value : json.dumps(value).encode('utf-8')
)


while(True):
    producer.send("hello", TOPIC)
    print("sent hello")
