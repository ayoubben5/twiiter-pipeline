import pandas as pd
import numpy as np
import tweepy
import json
from datetime import datetime
import s3fs
from pyhocon import ConfigFactory


def run_twitter_etl():
    conf = ConfigFactory.parse_file('./config/config.conf')
    API_KEY = conf.get('API_KEY')
    API_SECRET_KEY = conf.get('API_SECRET_KEY')
    BEERER = conf.get('BEERER')
    Access_Token = conf.get('Access_Token')
    Access_Token_Secret = conf.get('Access_Token_Secret')


    auth = tweepy.OAuth1UserHandler(
    API_KEY, API_SECRET_KEY, Access_Token, Access_Token_Secret
    )


    api = tweepy.API(auth)

    mnistre_username=['@Ouah1Abdellatif','@MoroccanGov','@FettahNadia','@CH_Benmoussa','@elmansourif75','@younessekkouri','@MezzourR','@MiraouiAbdeltif','@LeilaRBenali','@mehdibensaid','@mohcine_jazouli','@MustaphaBaitas','@MezzourGhita']

    tweet_list=[]
    for ministre in mnistre_username:
        tweets = api.user_timeline(screen_name=ministre,count=200,tweet_mode="extended",include_rts=False)
        for tweet in tweets:
            text = tweet._json['full_text']
            # print(tweet.user.description)
            refined_tweets= { "user": tweet.user.screen_name,
            'ministre' : tweet.user.description,
            "tweet":text,
            'favorite_count' :  tweet.favorite_count,
            'retweet_count' : tweet.retweet_count,
            'created_at': tweet.created_at ,
            'langue': tweet.lang,
            'followers_count' : tweet.user.followers_count

            }
            tweet_list.append(refined_tweets)
    
    df = pd.DataFrame(tweet_list)

    df.to_csv('s3://ayoub-airflow-twitter-bucket/MinistersTweeterData.csv',sep=';',index=False)