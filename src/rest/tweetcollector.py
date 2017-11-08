import sys
sys.path.append("../lib")
import api
import db
import tweepy
import yaml
from queue import Queue
import logging
import logging.config
from threading import Thread, Lock
import threading

class TweetCollector:
    def __init__(self, the_number_of_tweets):
        self.num = the_number_of_tweets
        with open("../lib/config/logging.yml","r") as f:
            logging.config.dictConfig(yaml.load(f))
        self.logger = logging.getLogger('TweetCollector')

    def collectTweets(self, unique_user_id):
        data = []
        def collect():
            count = 0
            with api.APIManager() as am:
                tweepy_cursor = tweepy.Cursor(am.api.user_timeline,
                                              id=unique_user_id,
                                              count=200).items()
                try:
                    for d in tweepy_cursor:
                        if d.text.find('RT') == 0:
                            continue
                        data.append(d)
                        count += 1
                        if self.num <= count:
                            return True
                    return True

                except tweepy.error.TweepError as err:
                    self.logger.warning(err.reason)
                    if err.reason == 'Twitter error response: status code = 401':
                        return True
                    else:
                        time.sleep(10)
                        return False

        while not collect():
            data = []

        return data

class TweetCollectorByThreading(TweetCollector):
    def __init__(self, the_number_of_tweets, input_table_name, output_table_name):
        super().__init__(the_number_of_tweets)
        self.thread_num = 4
        self.users_id_queue = Queue()
        self.input_table_name = input_table_name
        self.output_table_name = output_table_name
        connector = db.DB()
        with connector.connection.cursor() as cursor:
            sql = 'SELECT id FROM ' + input_table_name + ';'
            cursor.execute(sql)
            for c in cursor.fetchall():
                self.users_id_queue.put(c["id"])

        self.max_size = self.users_id_queue.qsize()
        with open("../lib/config/logging.yml","r") as f:
            logging.config.dictConfig(yaml.load(f))
        self.logger = logging.getLogger('TweetCollector.ByThreading')

    def setTweetInfoToDB(self,raw_info):
        info = (raw_info.id_str, raw_info.user.id_str, raw_info.created_at, raw_info.text)
        value = ' (id, user_id, time, tweet) '
        sql = 'INSERT IGNORE INTO ' + self.output_table_name + value + 'VALUES '
        sql += '(%s, %s, %s, %s)'
        check_sql = 'UPDATE ' + self.input_table_name + ' SET collect_tweet = true '
        check_sql += 'WHERE id = ' + raw_info.user.id_str
        connector = db.DB()
        with connector.connection.cursor() as cursor:
            cursor.execute(sql, info)
            connector.connection.commit()
            cursor.execute(check_sql)
            connector.connection.commit()

    def worker(self):
        while True:
            unique_user_id = self.users_id_queue.get()
            raw_info = self.collectTweets(unique_user_id)
            for i in raw_info:
                self.setTweetInfoToDB(i)
            self.users_id_queue.task_done()
            now_size = self.users_id_queue.qsize()
            progress_rate = round((self.max_size - now_size) / self.max_size, 4)
            self.logger.info("Progress rate:{0}%".format(progress_rate))

    def configureThreads(self):
        self.threads = []
        for roop in range(self.thread_num):
            t = Thread(target = self.worker)
            t.daemon = True
            t.start()
            self.threads.append(t)

    def run(self):
        self.configureThreads()
        self.users_id_queue.join()
                    
if __name__ == '__main__':
    tc = TweetCollectorByThreading(100, "random_users", "random_tweets")
    tc.run()
