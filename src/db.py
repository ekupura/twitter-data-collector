import yaml
import tweepy
import pymysql.cursors
import warnings
import pymysql

class DB:
    def __init__(self):
        config = {}
        with open("./config/config.yml","r") as f:
            config = yaml.load(f)
        self.config = config['db']

        self.connection = pymysql.connect(host = config["db"]["host"],
                                     user = config["db"]["user"],
                                     password = config["db"]["password"],
                                     db = config["db"]["database"],
                                     charset = config["db"]["charset"],
                                     cursorclass = pymysql.cursors.DictCursor)

    def insertUserInfomation(self, info, table_name = 'users'):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.connection.cursor() as cursor:
                sql = 'INSERT IGNORE INTO '+ table_name + ' VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
                cursor.execute(sql,info)
                self.connection.commit()

    def makeUsersTable(self, table_name = 'users'):
        with self.connection.cursor() as cursor:
            sql = "CREATE TABLE " + table_name + "(id varchar(64) PRIMARY KEY ,screen_name varchar(256), followers_count int, friends_count int, statuses_count int, description varchar(2048), url varchar(256), nit int) CHARACTER SET = 'utf8mb4'"
            cursor.execute(sql)
            self.connection.commit()


