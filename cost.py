import pandas as pd
import pymysql
from report import Function


def reset():
    cost = pd.read_excel("cost_init.xlsx")
    cost.columns = ['part', 'name', 'amount', 'cost', 'Unnamed: 4', 'Unnamed: 5']
    db = pymysql.connect(host='localhost', port=3306, user='root', password='root', db='cost', charset='utf8')
    cur = db.cursor()
    cur.execute("delete from licai")
    for x in cost.index:
        cur.execute("insert into licai values('" + str(cost.loc[x, 'part']) + "','" + str(
            cost.loc[x, 'name']) + "','" + str(cost.loc[x, 'cost']) + "','" + str(
            cost.loc[x, 'amount']) + "')")
    db.commit()


if __name__ == '__main__':
    Function('理财事业部').get_stream()()
