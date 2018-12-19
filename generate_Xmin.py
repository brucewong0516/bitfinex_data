# -*- coding: utf-8 -*-
"""
@Date   : 2018/10/11 9:28
@Author : Jack
@Content: 数字货币mongodb的1分钟数据合成 "5Min", "15Min", "30Min", "1H", "6H", "12H", "Day", "Week"
"""
import pymongo
import time
import datetime
import digital_data.dataObject as do

##################################################################
# ----------------------------------------------------------------
def get_all_contract(dbName="Digital_1min_Db"):
    """获取所有的数字货币合约"""
    client = pymongo.MongoClient()
    db = client[dbName]
    contractList = db.list_collection_names()
    return contractList

# ----------------------------------------------------------------
class BarGenerator(object):
    """x分钟K线的合成"""
    # --------------------------------------------------------------------------
    def __init__(self, basemin="1min", xmin="5min", contract="okex.BTCUSD_thisweek"):
        """
        构造器，输入参数：
        (1) basemin: 基础周期
        (2) xmin: 合成的分钟数,有: "5min", "15min", "30min", "1hour", "6hour", "12hour", "1day", "1week"
        (3) contract: 合约代码, 如"okex.BTCUSD_thisweek"
        """
        # 储存参数
        self.basemin = basemin
        self.xmin = xmin
        self.contract = contract
        # 数据库相关
        self.client = pymongo.MongoClient()  # 连接数据库
        self.dbName1 = "Digital_{}_Db".format(basemin)  # 基础分钟数据库
        self.dbName2 = "Digital_{}_Db".format(xmin)     # X分钟数据库
        self.collection1 = self.client[self.dbName1][contract]  # 某合约1分钟K线数据表格
        self.collection2 = self.client[self.dbName2][contract]  # 某合约X分钟K线数据表格
        self.collection2.ensure_index([('datetime1', pymongo.ASCENDING)], unique=True)
        # 分钟K线对象
        self.xBar = None

    # --------------------------------------------------------------------------
    def run(self):
        """执行输入"""
        # ----------------------------------------------------
        # 初始化
        start = time.time()  # 记录开始时间
        print(u"开始合成X分钟K线, 合约: {}, 周期: {}, 基础K线: {}".format(self.contract,
                                                            self.xmin, self.basemin))
        # ----------------------------------------------------
        # 循环1分钟K线,并生成x分钟K线,插入数据库
        cursor = self.collection1.find().sort("datetime1")
        for d in cursor:
            self.__onBar(d)
        # 断开数据库连接
        self.client.close()
        # 打印结果
        print(u"合成完毕, 耗时: {:.2f}s, {}".format(time.time() - start, self.contract))

    # --------------------------------------------------------------------------
    def __onBar(self, d):
        """输入base分钟K线dict，合成x分钟K线"""
        # 初始化K线
        if not self.xBar:
            # 初始化K线
            self.xBar = do.CtaBarData() # 创建K线实例
            # 添加初始数据
            # 基本信息
            self.xBar.gatewayName = d["gatewayName"]  # 数据来源
            self.xBar.symbol = d["symbol"]      # 合约代码
            self.xBar.exchange = d["exchange"]  # 交易所
            self.xBar.frequency = self.xmin     # 频率
            # 开始日期
            self.xBar.datetime1 = d["datetime1"]
            # K线数据
            self.xBar.open = float(d["open"])
            self.xBar.high = float(d["high"])
            self.xBar.low = float(d["low"])
            self.xBar.volume = float(d["volume"])
            self.xBar.amount = float(d["amount"])
        # 更新K线,常规更新
        else:
            self.xBar.high = max(self.xBar.high, float(d["high"]))
            self.xBar.low = min(self.xBar.low, float(d["low"]))
            self.xBar.volume += float(d["volume"])
            self.xBar.amount += float(d["amount"])

        # 结束K线判断
        # 结束时刻的小时数和分钟数
        h = d["datetime2"].hour    # 小时数
        m = d["datetime2"].minute  # 分钟数
        # 结束判断
        # "5min", "15min", "30min", "1hour", "6hour", "12hour", "1day", "1week"
        if self.xmin == "5min":
            is_end = not m % 5
        elif self.xmin == "15min":
            is_end = not m % 15
        elif self.xmin == "30min":
            is_end = not m % 30
        elif self.xmin == "1hour":
            is_end = not m % 60
        elif self.xmin == "6hour":
            a1 = h in [0,6,12,18]
            a2 = m == 0
            is_end = a1 and a2
        elif self.xmin == "12hour":
            a1 = h in [0,12]
            a2 = m == 0
            is_end = a1 and a2
        elif self.xmin == "1day":
            a1 = h == 0
            a2 = m == 0
            is_end = a1 and a2
        elif self.xmin == "1week":
            a1 = h == 0
            a2 = m == 0
            a3 = d["datetime2"].weekday() == 0
            is_end = a1 and a2 and a3
        else:
            print(u"xim无法识别")
            return

        # 结束计算最终分钟K线
        if is_end:
            # 更新结束时刻的数据
            self.xBar.datetime2 = d["datetime2"]                     # 结束时间
            self.xBar.date = self.xBar.datetime1.strftime("%Y%m%d")  # 日期
            # 量价数据
            self.xBar.close = float(d["close"])                # 最后的收盘价
            self.xBar.openInterest = float(d["openInterest"])  # 最后的持仓量
            # 插入数据库
            flt = {'datetime1': self.xBar.datetime1}
            self.collection2.replace_one(flt, self.xBar.__dict__, upsert=True)
            # 打印时间
            print(self.xBar.datetime2)
            # 清空x分钟bar
            self.xBar = None

# ----------------------------------------------------------------
def generate_all_Bar():
    """合成所有合约的所有周期的x分钟K线"""
    # 获取所有合约名称
    c_list= get_all_contract()
    # 设置需要合成的周期
    period_list = ["5Min", "15Min", "30Min", "1H", "6H", "12H", "Day", "Week"]
    # 所有合约循环
    for c in c_list:
        # # 选择指定合约
        # if c not in ["okex.BTCUSD_quarter"]:
        #     continue
        # 所有周期循环
        for p in period_list:
            bg = BarGenerator(xmin=p, contract=c)
            bg.run()
            del bg


##################################################################
if __name__ == "__main__":
    # ------------------------------------------------------------
    # 获取所有合约名称
    # contractList = get_all_contract()
    # print(contractList)
    # ------------------------------------------------------------
    # 合成所有合约的所有周期的x分钟K线
    # generate_all_Bar()
    # ------------------------------------------------------------
    # 合成Bitmex的分钟K线数据
    # bg = BarGenerator(basemin="5min", xmin="15min", contract="Bitmex.XBTUSD")
    # bg.run()
    for contract in ["Bitmex.XBTUSD","Bitmex.ETHUSD","Bitmex.XRPZ18"]:
        bg = BarGenerator(basemin="5min", xmin="15min", contract=contract)
        bg.run()


