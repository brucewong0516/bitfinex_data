# 数据库建立以后运行一次即可
# 建立索引（以时间戳）
if __name__ == '__main__':
    from Bitfinex_api2 import Bitfinex_api
    Bitfinex_api().create_index_mongo()