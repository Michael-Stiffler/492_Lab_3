import tweepy
from tweepy import Stream
import pandas as pd
from dotenv import load_dotenv
import os
from pymongo import MongoClient
import pymongo
import json
import sys


class writeTweepyDataToFile(tweepy.Stream):

    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
        self.counter = 0
        self.limit = 10000
        return super().__init__(consumer_key, consumer_secret, access_token, access_token_secret)

    def on_data(self, data):
        try:
            tweet_data = json.loads(data)
            with open("tweets_to_be_inserted.txt", "a", encoding='utf-8') as f:
                f.write(str(tweet_data) + "\n")
                f.close()
            self.counter += 1
            if self.counter < self.limit:
                collection.insert_one(tweet_data)
                return True
            else:
                stream.disconnect()
        except BaseException as e:
            print(e)

    def on_status(self, status):
        print(status)


if __name__ == "__main__":
    load_dotenv()

    access_token = os.getenv('ACCESS_TOKEN')
    access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
    consumer_token = os.getenv('API_KEY')
    consumer_token_secret = os.getenv('API_KEY_SECRET')

    connection = MongoClient('localhost', 27017)
    db = connection.LAB3
    collection = db.tweets
    db.tweets.create_index([("id", pymongo.ASCENDING)], unique=True)

    stream = writeTweepyDataToFile(consumer_key=consumer_token, consumer_secret=consumer_token_secret,
                                   access_token=access_token, access_token_secret=access_token_secret)
    stream.filter(track=["Ukraine Russian war",
                  "Russian Invasion"], languages=['en'])
