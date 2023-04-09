import tweepy
import pandas as pd
import s3fs

from datetime import datetime
import json

import authentication


def run_etl():
    consumer_key = authentication.CONSUMER_KEY
    consumer_secret = authentication.CONSUMER_SECRET
    access_token = authentication.ACCESS_TOKEN
    access_secret = authentication.ACCESS_SECRET

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@elonmusk',
                               count=200,
                               include_rts=False,
                               tweet_mode='extended')

    refined_tweets = []

    for tweet in tweets:
        txt = tweet._json["full_text"]
        refined_tweet = {
            "user": tweet.user.screen_name,
            "text": txt,
            "likes": tweet.favorite_count,
            "retweet_count": tweet.retweet_count,
            "created_at": tweet.created_at
        }
        refined_tweets.append(refined_tweet)

    df = pd.DataFrame(refined_tweets)
    df.to_csv("elonmusk_twitter_data.csv")
