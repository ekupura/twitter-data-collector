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

    def insertUserTweet(self, info, table_name = 'tweets'):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.connection.cursor() as cursor:
                sql = 'INSERT IGNORE INTO '+ table_name + ' VALUES (%s, %s, %s)'
                cursor.execute(sql,info)
                self.connection.commit()

    def getUsersID(self, table_name = 'users'):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.connection.cursor() as cursor:
                sql = 'SELECT id FROM ' + table_name + ';'
                cursor.execute(sql)
                return cursor.fetchall()

    def getUsersName(self, table_name = 'users', where_column=None):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.connection.cursor() as cursor:
                sql = 'SELECT screen_name FROM ' + table_name
                if not where_column is None:
                    sql += ' WHERE ' + where_column + ' = 0'
                cursor.execute(sql)
                return [x["screen_name"] for x in cursor.fetchall()]

    def updateColumn(self, _id, value, column_name, table_name = 'users'):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with self.connection.cursor() as cursor:
                sql = 'UPDATE ' + table_name 
                sql += ' SET ' + column_name
                sql += ' = %s WHERE id = %s' 
                cursor.execute(sql,(value, _id))
                self.connection.commit()


    def __del__(self):
        self.connection.close()



