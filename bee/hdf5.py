#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/10 15:53
# @Author  : Dominolu
# @File    : hdf5.py
# @Software: PyCharm

import pandas as pd
import pymysql
import datetime
from bee.config import cfg
db=pymysql.connect(host=cfg.mysql_host,port=3306,user='root',password='root_pwd',database='bee',charset='utf8')
# sql = "select * from liquidation"
# data=pd.read_sql(sql,con=db,index_col="ts")
# store = pd.HDFStore('store.h5')
# store["df"]=data
#
#
# store.close()
#
#
# data.to_hdf("store.h5",key='df', mode='w')



def main():
    sql="select * from trades where broker=%s and date(ts)=%s"
    dt=get_date()
    for i in dt:
        min_date=i[1]
        today = datetime.date.today()
        f = cfg.path  +"/" + str(min_date)+".h5"
        store = pd.HDFStore(f, mode="w",complevel=9)
        if min_date<today:
            data = pd.read_sql(sql, con=db, params=i,index_col="ts")
            store[i[0]] = data
            # with db.cursor() as cursor:
            #     cursor.execute(sql,i)
            #     data = cursor.fetchall()
            #     return list(data)
        store.close()



def get_date():
    sql="select broker,min(date(ts)) as 'dt' from trades group by broker;"
    with db.cursor() as cursor:
        cursor.execute(sql)
        data=cursor.fetchall()
        return list(data)


if __name__ == '__main__':
    data=main()
    print(data)