# -*- coding: utf-8 -*-
"""
@Date   : 2018/11/2 15:56
@Author : Jack
@Content: 检查数据是否连续
"""
import pymongo
from datetime import datetime, timedelta

def check_continue(symbol, frequency):
    # ----------------------------------------------------
    # 连接数据库
    # client = pymongo.MongoClient('localhost', 27017)
    client = pymongo.MongoClient("192.168.101.189", 27017)
    dbName = "crypto_{}".format(frequency)
    collection = client[dbName][symbol]
    clustor = collection.find().sort('start_time')
    # print(dbName, symbol)
    # ----------------------------------------------------
    # 时间间隔处理
    num = int(''.join(x for x in frequency if x.isdigit()))
    if frequency.endswith("min"):
        delta = timedelta(minutes=num)
    elif frequency.endswith("hour"):
        delta = timedelta(hours=num)
    elif frequency.endswith("day"):
        delta = timedelta(days=num)
    else:
        print(u"错误, 周期: {}无法识别".format(frequency))
        return

    # 循环检查
    last_dtList = None
    for d in clustor:
        dt1 = d["start_time"]
        dt2 = d["end_time"]
        # print(dt1)
        dtList = [dt1, dt2]
        # 初始化
        if last_dtList is None:
            last_dtList = dtList
            print(u"开始时间", dt1)
            continue
        # (1) dt1间隔检验(使用K线开始时间, dt1来检测)
        diff = dt1 - last_dtList[0] # 2根K线之间的间隔
        diff_num = diff/delta-1       # 相差多少根K线
        if diff_num > 1:
            print("dt1:{}和dt1:{}之间差了{}跟K线".format(last_dtList[0], dt1, diff_num))
        # 更新上个K线的时间
        last_dtList = dtList

    client.close()


if __name__ == "__main__":
    check_continue("bitfinex_btc_usd", "1min")