#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/3 11:36
# @Author  : Dominolu
# @File    : tet.py
# @Software: PyCharm
import pymysql
import json
conn=pymysql.connect(host="10.0.10.3",user="market",password="c6wwB4d3rABJFwkH",db="market")
sql="INSERT INTO liquidation (broker, symbol, side, volume, price, avgprice, ts) VALUES (%s,%s,%s,%s,%s,%s,%s)"
input=open("/root/liquidation.sql","r")
line=input.readline()
data=[]
i=0
while line:
    line=line.replace("),\n",")")
    line = line.replace("(", '[')
    line = line.replace(")", ']')
    line = line.replace("'", '"')
    data.append(json.loads(line))
    i=i+1
    if i>1000:
        cursor = conn.cursor()
        cursor.executemany(sql, data)
        conn.commit()
        i = 0
        data.clear()
    line = input.readline()


if i>0:
    cursor = conn.cursor()
    cursor.executemany(sql, data)
    conn.commit()



[{"broker":'Binance', "symbol":'DOGEUSDT', "side":'SELL', "price":58075, "volume":0.160586, "avgprice":0.161003, "ts":'2022-04-26 17:07:24'}, {"broker":'Binance', "symbol":'XMRUSDT', "side":'SELL', "price":0.463, "volume":258.09, "avgprice":259.55, "ts":'2022-04-26 17:07:24'}]

[('Binance', 'DOGEUSDT', 'SELL', 58075, 0.160586, 0.161003, '2022-04-26 17:07:24'), ('Binance', 'XMRUSDT', 'SELL', 0.463, 258.09, 259.55, '2022-04-26 17:07:24')]