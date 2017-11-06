from api import generateAPIs
import tweepy

apis = generateAPIs()
api = apis.get()
try:
    api.user_timeline(id=1000903189)
except tweepy.error.TweepError as err:
    print('aaa')
    pass

