import pandas as pd
import pymysql

data = pd.read_excel('data/发行历史.xls')
data = data[data['缴款状态'] == '缴款成功']
db = pymysql.connect(host='localhost', port=3306, user='root', password='root', db='report', charset='utf8')
cur = db.cursor()
cur.execute("drop table if exists issue")
sql = "create table issue ("
for x in data.columns.tolist():
    x = x.split("/")[0]
    x = x.split("(")[0]
    sql += x + " varchar(255),"
sql = sql[:-1] + ")"
cur.execute(sql)
db.commit()
for x in data.index.tolist():
    sql = "insert into issue values('"
    for y in data.columns.tolist():
        sql += str(data.loc[x, y]) + "','"
    sql = sql[:-2] + ")"
    cur.execute(sql)
db.commit()
db.close()
