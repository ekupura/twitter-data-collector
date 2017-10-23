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
import time



class TweetCollector(Collector):
    def __init__(self):
        super().__init__()
        db_connector = db.DB()
        name_list = db_connector.getUsersName(where_column = 'search')
        self.user_id_queue = Queue()
        for name in name_list:
            self.user_id_queue.put(name)
        self.logger = logging.getLogger('collector.search')
        self.keyword = ["高専", "procon", "roboron", "プロコン", "ロボコン"]

    def collect(self, tweepy_cursor, cur, data):
        now_cursor = cur
        try:
            for d in tweepy_cursor:
                data.append(d)
                now_cursor = tweepy_cursor.page_iterator.index
            return (True, -1)

        except tweepy.error.TweepError as err:
            self.logger.warning(err)
            time.sleep(10)
            return (False, now_cursor)

    def requirement(self, data):
        if data.text.find('RT') == 0:
            return False
        return True

    def getData(self, user_id):
        cur = -1
        data = []
        query = user_id + ' AND ' + ' OR '.join(self.keyword),
        while True:
            with APIManager() as am:
                tweepy_cursor = tweepy.Cursor(am.api.search,
                                              q=query,
                                              count=1).items()
                result = self.collect(tweepy_cursor, cur, data)
                if result[0]:
                    return data

    def setDataToDB(self, db_connector, data, thread_id = ''):
        info = (data.id_str, data.user.id_str, data.text)
        db_connector.insertUserTweet(info, 'search')

    def setMarkToDB(self, db_connector, user_id):
        db_connector.updateColumn(user_id, 1, 'search')

    def run(self):
        self.configureThreads()
        self.user_id_queue.join()

if __name__ == '__main__':
    i = TweetCollector()
    i.run()
