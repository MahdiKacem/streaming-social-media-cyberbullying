from kafka import KafkaProducer
import argparse
from dotenv import load_dotenv
from googleapiclient.discovery import build
import os
import json

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
TOPIC = "social_stream"
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

def connect_to_kafka():

    producer = KafkaProducer(
                bootstrap_servers = bootstrap_servers,
                value_serializer = lambda value : json.dumps(value).encode('utf-8')
    )
    return producer

def push_to_kafka(comments, topic = TOPIC):
    producer = connect_to_kafka()
    for comment in comments:
        producer.send(topic, comment)
        print("Comment sent to kafka")
    producer.flush()
    print('All comments sent to kafka')

def get_video_comments(video_id, api_key = API_KEY):
    youtube = build(
        "youtube", "v3", developerKey = api_key
    )

    request = youtube.commentThreads().list(
        part = "snippet, replies",
        videoId = video_id,
    )

    response = request.execute()
    comments = []
    while response:
        for item in response["items"]:
            comment_snippet = item["snippet"]["topLevelComment"]["snippet"]
            reply_count = item["snippet"]["totalReplyCount"]
            comment_replies = []
            if reply_count > 0:
                for reply in item['replies']['comments']:
                    reply = reply['snippet']['textDisplay']
                    comment_replies.append(reply)
            comments.append({
                "authorDisplayName": comment_snippet["authorDisplayName"],
                "publishedAt": comment_snippet["publishedAt"],
                "textOriginal": comment_snippet["textOriginal"],
                "likeCount": comment_snippet["likeCount"],
                "replies" : comment_replies
            })
        if 'nextPageToken' in response:
            response = youtube.commentThreads().list(
                    part = 'snippet,replies',
                    videoId = video_id,
                      pageToken = response['nextPageToken']
                ).execute()
        else:
            break
    return comments

#video_id = "KO21YWW-YmI"
#print(get_video_comments(video_id = video_id))
#while(True):
#    producer.send("hello", TOPIC)
 #   print("sent hello")
if __name__ == "__main__":
    comments = get_video_comments("NHw1KCCODsw")
    push_to_kafka(comments)
