import api
import db
import yaml
import tweepy
from threading import Thread, Lock
import threading
from queue import Queue
import logging
import logging.config


class APIManager:
    apis = api.generateAPIs()

    def __init__(self):
        self.api = APIManager.apis.get()
    
    def __enter__(self):
        return self

    def qsize(self):
        return APIManager.apis.qsize()

    def __exit__(self, type, value, traceback):
        APIManager.apis.put(self.api)

class Collector:
    def __init__(self):
        self.thread_num = 8
        self.user_id_queue = Queue()
        self.root_name = 'in9b_09'
        with open("./config/logging.yml","r") as f:
            logging.config.dictConfig(yaml.load(f))
        self.logger = logging.getLogger('collector')

    # Please override into your inherited class
    def requirement(self, user_data):
        return True

    def collect(self, tweepy_cursor, cur, data):
        now_cursor = cur
        try:
            for d in tweepy_cursor:
                data.append(d)
                now_cursor = tweepy_cursor.page_iterator.next_cursor
            return (True, -1)

        except tweepy.error.TweepError :
            self.logger.warning("API Limited. Change api.")
            return (False, now_cursor)

    # Please override into your inherited class
    def getData(self, user_id):
        cur = -1
        data = []
        while True:
            with APIManager() as am:
                tweepy_cursor = tweepy.Cursor(am.api.followers,
                                              id=user_id,
                                              count=200,
                                              cursor=cur).items()
                result = self.collect(tweepy_cursor, cur, data)
                if result[0]:
                    return data
                cur = result[1]

    # Please override into your inherited class
    def setDataToDB(self, db_connector, data, thread_id = ''):
        info = (data.id_str, data.screen_name, data.followers_count)
        info += (data.friends_count, data.statuses_count)
        self.logger.debug(thread_id+'Attempt to set data into the database...')
        db_connector.insertUserInfomation(info, 'collector_test')
        self.logger.debug(thread_id+'Data was successfully inserted into the database!')

    # Please override into your inherited class
    def additionalAction(self, datum):
        pass

    def workerToGetData(self): 
        db_connector = db.DB()
        thread_id = '[' + str(threading.get_ident()) + '] '
        while True:
            user_id = self.user_id_queue.get()
            self.logger.debug(thread_id+"Attempt to get data...")
            data = self.getData(user_id)
            self.logger.debug(thread_id+"[Twitter] OK!")
            try:
                for datum in data:
                    if self.requirement(datum):
                        self.additionalAction(datum)
                        self.setDataToDB(db_connector, datum, thread_id)
                self.logger.debug(thread_id+"Task Done!")

            except pymysql.err.OperationalError as err:
                self.logger.error(thread_id+'{0}'.format(err))
                
            finally :
                self.user_id_queue.task_done()

    def configureThreads(self):
        self.threads = []
        for roop in range(self.thread_num):
            t = Thread(target = self.workerToGetData)
            t.daemon = True
            t.start()
            self.threads.append(t)

    # Please override into your inherited class
    def run(self):
        self.user_id_queue.put(self.root_name)
        self.configureThreads()
        self.user_id_queue.join()

if __name__ == '__main__':
    c = Collector()
    c.run()
