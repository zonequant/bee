#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/13 10:04
# @Author  : Dominolu
# @File    : download.py
# @Software: PyCharm
import time

import requests
import pandas as pd
from loguru import logger as log

path="/app/data"
host="https://fapi.binance.com"
def get_symbols():
    url="/fapi/v1/exchangeInfo"
    reponse=requests.get(host+url)
    data=reponse.json()
    return data["symbols"]

def main():
    param=""
    symbols=get_symbols()
    starttime=1648742400000
    url="/fapi/v1/klines"
    for  i in symbols:
        last=starttime
        data=[]
        while last<1661961600000:
            try:
                param={"symbol":i["symbol"],"interval":"1m","startTime":last,"limit":1000}
                respone=requests.get(host+url,params=param)
                dt=respone.json()
                for j in dt:
                    item=[j[0],float(j[1]),float(j[2]),float(j[3]),float(j[4]),float(j[5]),j[6],float(j[7]),float(j[8]),float(j[9]),float(j[10]),j[11]]
                    data.append(item)
                last=dt[-1][0]
                time.sleep(0.5)
            except:
                log.error(f"{param}-error")
        data=pd.DataFrame(data,columns=["ts","open","high","low","close","volume","t1","amount","trades","buy","sell","t2"])
        data.to_hdf(path+"/binance_1m.h5",i["symbol"],complevel=5)
        log.info(f"{i['symbol']} is complete.")

if __name__ == '__main__':
    main()