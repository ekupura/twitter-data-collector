from search import SearchCollector
import csv
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
from collections import defaultdict

class SearchEmojiCollector(SearchCollector):
    def __init__(self):
        super().__init__()
        self.user_id_queue = Queue()
        db_connector = db.DB()
        with db_connector.connection.cursor() as cursor:
            sql = 'SELECT id, emoji FROM emoji_list '
            cursor.execute(sql)
            data = cursor.fetchall()
            sql = 'SELECT emoji_id, count(emoji_id) AS emoji_count FROM emoji_tweets GROUP BY emoji_id'
            cursor.execute(sql)
            check = defaultdict(int)
            for c in cursor.fetchall():
                check[c["emoji_id"]] = c["emoji_count"]

            for i, row in enumerate(data):
                if check[i] > 10000:
                    continue
                self.user_id_queue.put(row)
        self.logger = logging.getLogger('collector.search.emoji')

    def requirement(self, data):
        return True

    def collect(self, api, query, data):
        cursor = tweepy.Cursor(api.search, q=query, count=100, lang='ja').items()
        try:
            for d in cursor:
                data.append(d)

        except tweepy.error.TweepError as err:
            self.logger.warning(err)
            if err.reason.find('code = 429') != -1:
                return True
            else:
                return False

    def getData(self, emoji, thread_id):
        page = 1
        data = []
        query = emoji
        result = False
        with APIManager() as am:
            while not result:
                data = []
                result = self.collect(am.api, query, data)
        return data

    def getSingleUserData(self, thread_id='', user_id=None):
        emoji_dict = self.user_id_queue.get()
        emoji = emoji_dict['emoji']
        emoji_id = emoji_dict['id']
        self.logger.info(thread_id+"Qsize:{0}".format(self.user_id_queue.qsize()))
        self.logger.debug(thread_id+"Attempt to get data...")
        data = self.getData(emoji, thread_id)
        self.logger.debug(thread_id+"[Twitter] OK!")
        try:
            db_connector = db.DB()
            for datum in data:
                if self.requirement(datum):
                    self.additionalAction(datum, thread_id)
                    self.setDataToDB(db_connector, datum, emoji_id, thread_id)

            self.setMarkToDB(db_connector, emoji_id)
            self.logger.debug(thread_id+"Task Done!")

        except pymysql.err.OperationalError as err:
            self.logger.error(thread_id+'{0}'.format(err))
            self.user_id_queue.put(emoji_dict)

        finally :
            time.sleep(45)
            self.user_id_queue.task_done()

    def setDataToDB(self, db_connector, data, emoji_id, thread_id = ''):
        info = (data.id_str, emoji_id, data.text)
        with db_connector.connection.cursor() as cursor:
            sql = 'INSERT IGNORE INTO emoji_tweets (tweet_id, emoji_id, tweet) VALUES (%s, %s, %s)'
            cursor.execute(sql,info)
            db_connector.connection.commit()

    def setMarkToDB(self, db_connector, emoji_id):
        with db_connector.connection.cursor() as cursor:
            sql = 'UPDATE emoji_list SET check1 = 1 WHERE id = %s'
            cursor.execute(sql,emoji_id)
            db_connector.connection.commit()



if __name__ == '__main__':
    i = SearchEmojiCollector()
    i.run()

        
