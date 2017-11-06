import csv
from db import DB

emoji_list = []
with open("emoji.csv","r") as f:
    reader = csv.reader(f)
    for row in reader:
        emoji_list.append(row[0])

db = DB()
with db.connection.cursor() as cursor:
    sql = 'INSERT INTO emoji_list VALUE (%s, %s, 0)'
    for i, v in enumerate(emoji_list):
        info = (i, v)
        cursor.execute(sql,info)
        db.connection.commit()



