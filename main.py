import tweepy
import pandas as pd 
from dotenv import load_dotenv
import os

def main():
    load_dotenv()
    auth = tweepy.OAuthHandler(os.getenv('API_KEY'), os.getenv('API_KEY_SECRET'))
    auth.set_access_token(os.getenv('ACCESS_TOKEN'), os.getenv('ACCESS_TOKEN_SECRET'))
    api = tweepy.API(auth)
    
    cursor = tweepy.Cursor(api.user_timeline, id="dapr", tweet_mode="extended").items(1)
    
    for i in cursor:
        print(i)

if __name__ == "__main__":
    main()