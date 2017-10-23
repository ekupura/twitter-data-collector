import api
import db
import yaml
import tweepy
from threading import Thread, Lock
import threading
from queue import Queue
import logging
import logging.config
import pymysql
from collector import Collector
from collector import APIManager
import pprint

class TweetCollector(Collector):
    keyword = ["高専", "procon", "roboron", "プロコン", "ロボコン"]
    def __init__(self):
        super().__init__()
        db_connector = db.DB()
        id_list = db_connector.getUsersID()
        self.user_id_queue = Queue()
        for _id in id_list:
            self.user_id_queue.put(_id["id"])
        self.logger = logging.getLogger('collector.tweet')

    def collect(self, tweepy_cursor, cur, data):
        now_cursor = cur
        try:
            for d in tweepy_cursor:
                data.append(d)
                now_cursor = tweepy_cursor.page_iterator.index
                return (True, -1)
            return (True, -1)

        except tweepy.error.TweepError :
            self.logger.warning("API Limited. Change api.")
            return (False, now_cursor)

    def requirement(self, data):
        if data.text.find('RT') == 0:
            return False
        return True

    def getData(self, user_id):
        cur = -1
        data = []
        while True:
            with APIManager() as am:
                tweepy_cursor = tweepy.Cursor(am.api.user_timeline,
                                              id=user_id,
                                              count=200,
                                              page=cur).items()
                result = self.collect(tweepy_cursor, cur, data)
                if result[0]:
                    return data
                cur = result[1]

    def setDataToDB(self, db_connector, data, thread_id = ''):
        info = (data.id_str, data.user.id_str, data.text)
#        self.logger.debug(thread_id+'Attempt to set data into the database...')
        self.logger.debug(thread_id+"id:"+data.user.screen_name)
        db_connector.insertUserTweet(info, 'tweets')
#        self.logger.debug(thread_id+'Data was successfully inserted into the database!')

if __name__ == '__main__':
    i = TweetCollector()
    i.run()
