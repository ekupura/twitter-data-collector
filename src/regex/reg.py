import sys
sys.path.append('../lib')
import re
import pymysql
import pymysql.cursors

class Regex:
    def replace(self, string):
        # exclude RT, url, symbol, id
        ex1 = r'(^RT)?(https?:/+([\/a-zA-Z0-9一-龠_ぁ-ん_ァ-ヴーａ-ｚＡ-Ｚ\-_\.!\*\'\(\)])*)*[\.\^\$\*\+\?\{\}\[\]\\\|\(\)\'\"!%&-=`:;<>,.?/_]*((@|#)[一-龠_ぁ-ん_ァ-ヴーａ-ｚＡ-Ｚa-zA-Z0-9_]*)*[^一-龠_ぁ-ん_ァ-ヴーａ-ｚＡ-Ｚa-zA-Z0-9 　。.．,]*'
        ex2 = r'[\n。.．,　]+'
        re_string = re.sub(ex2, ' ', re.sub(ex1, '', string))
        return re_string
        
    def makeConnection(self):
        return pymysql.connect(host="mariadb", user="root", 
                               passwd="admin", db="twitter",
                               charset="utf8mb4",
                               cursorclass = pymysql.cursors.DictCursor)
        
    def run(self, input_table, output_table):
        g_sql = "select * from " + input_table
        p_sql = "insert ignore into "+output_table+" value (%s, %s, %s, %s)"
        # Fetch
        connection = self.makeConnection()
        with connection.cursor() as cursor:
            cursor.execute(g_sql)
            row = cursor.fetchall()
        connection.close()
        print("fetch OK")
        # Replace
        replaced_info = [[r["id"], r["user_id"], r["time"], self.replace(r["tweet"])] for r in row]
        print("replace OK")
        # Commit
        connection = self.makeConnection()
        with connection.cursor() as cursor:
            cursor.executemany(p_sql, replaced_info)
            connection.commit()
        connection.close()
        print("commit OK")

                    
if __name__ == '__main__':
    r = Regix()
    r.run("test", "test_out")
