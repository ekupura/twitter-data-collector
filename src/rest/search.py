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



class SearchCollector(Collector):
    def __init__(self):
        super().__init__()
        db_connector = db.DB()
        name_list = db_connector.getUsersName(where_column = 'search')
        self.user_id_queue = Queue()
        for name in name_list:
            self.user_id_queue.put(name)
        self.logger = logging.getLogger('collector.search')
        self.keyword = ["高専", "procon", "roboron", "プロコン", "ロボコン"]

    def requirement(self, data):
        if data.text.find('RT') == 0:
            return False
        return True

    def collect(self, api, query, page, data):
        while page < 100:
            try:
                d = am.api.search(q=query, count=100, page = page).items()
                page += 1
                data.append(d)

            except tweepy.error.TweepError as err:
                self.logger.warning(err)
                return page


    def getData(self, user_id, thread_id):
        page = 0
        data = []
        query = user_id + ' AND ' + ' OR '.join(self.keyword)
        while page < 100:
            with APIManager() as am:
                page = self.collect(tweepy_cursor, am, query, page, data)
        return data

    def setDataToDB(self, db_connector, data, thread_id = ''):
        info = (data.id_str, data.user.id_str, data.text)
        db_connector.insertUserTweet(info, 'search')

    def setMarkToDB(self, db_connector, user_id):
        db_connector.updateColumn(user_id, 1, 'search', name=True)

    def run(self):
        self.configureThreads()
        self.user_id_queue.join()

if __name__ == '__main__':
    i = SearchCollector()
    i.run()
