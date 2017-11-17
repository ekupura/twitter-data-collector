import sys
sys.path.append('../lib')
import re
import pymysql
import pymysql.cursors

def replace(string):
    # exclude RT, url, symbol, id
    ex1 = r'(^RT)?(https?:/+([\/a-zA-Z0-9一-龠_ぁ-ん_ァ-ヴーａ-ｚＡ-Ｚ\-_\.!\*\'\(\)])*)*[\.\^\$\*\+\?\{\}\[\]\\\|\(\)\'\"!%&-=`:;<>,.?/_]*((@|#)[一-龠_ぁ-ん_ァ-ヴーａ-ｚＡ-Ｚa-zA-Z0-9_]*)*[^一-龠_ぁ-ん_ァ-ヴーａ-ｚＡ-Ｚa-zA-Z0-9 　。.．,]*'
    ex2 = r'[\n。.．,　]+'
    re_string = re.sub(ex2, ' ', re.sub(ex1, '', string))
    return re_string
    
    return string

def main():
    connection = pymysql.connect(host="mariadb", user="root", 
                                 passwd="admin", db="twitter",
                                 charset="utf8mb4",
                                 cursorclass = pymysql.cursors.DictCursor)
    g_sql = "select * from ob_tweets"
    p_sql = "insert ignore into ob_tweets_removed_noise value (%s, %s, %s, %s)"
    with connection.cursor() as cursor:
        cursor.execute(g_sql)
        row = cursor.fetchall()
        print("fetch OK")
        replaced_info = [[r["id"], r["user_id"], r["time"], replace(r["tweet"])] for r in row]
        print("replace OK")
        cursor.executemany(p_sql, replaced_info)
        connection.commit()
        print("commit OK")
    connection.close()

                    
if __name__ == '__main__':
    main()
