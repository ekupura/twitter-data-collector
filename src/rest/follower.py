from collector import Collector
from queue import Queue
import logging
import pymysql

class FollowerCollector(Collector):
    def __init__(self):
        super().__init__()
        self.user_id_queue = Queue()
        self.lang = 'ja'
        self.root_name = 'imo_neg'
        self.next_user_id = []
        self.logger = logging.getLogger('collector.follower')

    def requirement(self, user_data):
        if user_data.lang != self.lang:
            return False
        if user_data.protected == True:
            return False
        return True

    def setDataToDB(self, db_connector, data, thread_id = ''):
        info = (data.id_str, data.screen_name, data.followers_count)
        info += (data.friends_count, data.statuses_count)
        info += (data.description, data.url, -1)
        self.logger.debug(thread_id+'Attempt to set data into the database...')
        db_connector.insertUserInfomation(info, 'users')
        self.logger.debug(thread_id+'Data was successfully inserted into the database!')

    def additionalAction(self, datum, thread_id):
        self.next_user_id.append(datum.id_str)
        self.logger.debug(thread_id+'qsize:{0}'.format(self.user_id_queue.qsize()))


    def run(self):

        self.user_id_queue.put(self.screenNameToID(self.root_name))
        self.configureThreads()
        for roop in range(2):
            self.logger.info('Phase:%d',roop)
            self.logger.info('Wait for the user id queue to become empty...')
            self.user_id_queue.join()
            self.logger.info('The user id queue become empty.')
            for next_id in self.next_user_id :
                self.user_id_queue.put(next_id)

        
        self.logger.info('All Task Done!')

if __name__ == '__main__':
    i = FollowerCollector()
    i.run()

