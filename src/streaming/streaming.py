import sys
sys.path.append("../lib")
import api as apiconfig
import db
import tweepy

def generateStreams():
    def write(user):
        connector = db.DB()
        sql = 'INSERT IGNORE INTO random_users '
        sql += 'VALUES (%s, %s, %s, %s, %s, %s, %s)'
        info = (user.id_str, user.screen_name, 
                user.followers_count, user.friends_count,
                user.statuses_count, user.description,
                user.url)
        with connector.connection.cursor() as cursor:
            cursor.execute(sql,info)
            connector.connection.commit()

    class MyStreamListener(tweepy.StreamListener):
        def on_status(self, status):
            if status.user.lang == 'ja':
                write(status.user)

        def on_error(self, status_code):
            if status_code == 420:
                return False
    
    streams = []
    for auth in apiconfig.generateAuth():
        myStreamListener = MyStreamListener()
        streams.append(tweepy.Stream(auth = auth, listener=myStreamListener))
    return streams


def main():
    streams = generateStreams()
    for stream in streams:
        try:
            stream.sample(async = False)
        except:
            pass

if __name__ == '__main__':
    main()
