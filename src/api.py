# -*- coding: utf-8 -*-
import csv
import yaml
from pprint import pprint
import tweepy
from queue import Queue 

def generateAuth():
    config = {}
    with open("./config/config.yml","r") as f:
        config = yaml.load(f)

    consumer_key = config['api']['consumer_key']
    consumer_secret = config['api']['consumer_secret']
    access_token = []
    access_token_secret = []

    with open(config['api']['access_token_path'], 'r') as f:
        for row in csv.reader(f):
            access_token.append(row[0])
            access_token_secret.append(row[1])

    auth_list = []
    for at, ats in zip(access_token, access_token_secret) :
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(at, ats)
        auth_list.append(auth)

    return auth_list

def generateAPIs():
    apis = Queue()
    for auth in generateAuth():
        apis.put(tweepy.API(auth, wait_on_rate_limit = False))

    return apis

def generateSingleAPI(access_token, access_token_secret):
    config = {}
    with open("./config.yml","r") as f:
        config = yaml.load(f)

    consumer_key = config['api']['consumer_key']
    consumer_secret = config['api']['consumer_secret']
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return (tweepy.API(auth, wait_on_rate_limit = False))






