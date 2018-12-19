# Bitfinex_api多线程版本
import queue
import requests
import json
import time
import pymongo
import datetime
from pymongo.errors import PyMongoError
import threading


class Bitfinex_api(object):

    def __init__(self):
        self.__base_headers = {
            'referer': 'https://www.bitfinex.com/',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
        }
        # 基础headers
        self.__symbol_detail_url = 'https://api.bitfinex.com/v1/symbols_details'
        self.__history_price_url = 'https://www.bitfinex.com/v2/candles/trade:'
        self.__client = pymongo.MongoClient('localhost', 27017)
        self.__db = self.__client['bitfinex']
        self.__symbol_detail_sheet = self.__db['symbol_detail']
        self.__price_type_list = ['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D']
        self.__price_type_limit = [60000, 300000, 900000, 1800000, 3600000, 10800000, 21600000, 43200000, 86400000]

    def __create_next_end_timestamp(self, price_type, end_timestamp):
        # 该函数的作用是在翻页时构建参数end
        if price_type == '1m':
            next_timestamp = end_timestamp - 60*1000
            return next_timestamp
        elif price_type == '5m':
            next_timestamp = end_timestamp - 300*1000
            return next_timestamp
        elif price_type == '15m':
            next_timestamp = end_timestamp - 900*1000
            return next_timestamp
        elif price_type == '30m':
            next_timestamp = end_timestamp - 1800*1000
            return next_timestamp
        elif price_type == '1h':
            next_timestamp = end_timestamp - 3600*1000
            return next_timestamp
        elif price_type == '3h':
            next_timestamp = end_timestamp - 10800*1000
            return next_timestamp
        elif price_type == '6h':
            next_timestamp = end_timestamp - 21600*1000
            return next_timestamp
        elif price_type == '12h':
            next_timestamp = end_timestamp - 43200*1000
            return next_timestamp
        elif price_type == '1D':
            next_timestamp = end_timestamp - 86400*1000
            return next_timestamp

    def get_all_symbol_detail(self):
        db_sheet_list = self.__db.list_collection_names()
        if 'symbol_detail' in db_sheet_list:
            pass
        else:
            # 获取所有的交易对的详细信息
            try:
                res = requests.get(url=self.__symbol_detail_url, headers=self.__base_headers)
                symbol_detail_data = json.loads(res.text)
                symbol_detail_sheet = self.__db['symbol_detail']
                symbol_detail_sheet.insert_many(symbol_detail_data)
                return symbol_detail_data
            except requests.RequestException:
                print('获取symbol_detail时网络出现错误！')

    def create_initially_urls_queue(self):
        # 初始创建url队列
        initially_urls_queue = queue.Queue()
        now_time = int(time.time()*1000)
        self.get_all_symbol_detail()
        symbol_pairs_list = [i['pair'] for i in self.__symbol_detail_sheet.find({}, {'pair': True, '_id': False})]
        initially_urls_list = [self.__history_price_url + price_type + ':' + 't' + symbol_pair.upper() + '/hist?end=' + str(now_time) + '&limit=5000&_bfx=1' for symbol_pair in symbol_pairs_list for price_type in self.__price_type_list]
        for initially_url in initially_urls_list:
            initially_urls_queue.put(initially_url)
        return initially_urls_queue

    def create_initially_update_urls_queue(self):
        # 初始创建更新的队列
        initially_update_urls_queue = queue.Queue()
        now_time = int(time.time()*1000)
        symbol_pairs_list = [i['pair'] for i in self.__symbol_detail_sheet.find({}, {'pair': True, '_id': False})]
        for symbol_pair in symbol_pairs_list:
            symbol_pair_sheet = self.__db[symbol_pair]
            for price_type in self.__price_type_list:
                db_end_timestamp = list(symbol_pair_sheet.find({'price_type': price_type}).sort('timestamp', pymongo.DESCENDING).limit(1))[0]['timestamp']
                limit = int((time.time() * 1000 - db_end_timestamp) / self.__price_type_limit[self.__price_type_list.index(price_type)]) - 1
                if limit >= 5000:
                    update_history_price_url = self.__history_price_url + price_type + ':' + 't' + symbol_pair.upper() + '/hist?end=' + str(now_time) + '&limit=5000&_bfx=1'
                    initially_update_urls_queue.put(update_history_price_url)
                elif 0 < limit < 5000:
                    update_history_price_url = self.__history_price_url + price_type + ':' + 't' + symbol_pair.upper() + '/hist?end=' + str(now_time) + '&limit=' + str(limit) + '&_bfx=1'
                    initially_update_urls_queue.put(update_history_price_url)
                elif limit <= 0:
                    pass
        return initially_update_urls_queue

    def create_drop_repeated_data_queue(self):
        drop_repeated_data_queue = queue.Queue()
        Bitfinex_db_collection_names = self.__db.list_collection_names()
        price_type_list = self.__price_type_list
        del Bitfinex_db_collection_names[Bitfinex_db_collection_names.index('symbol_detail')]
        for symbol_pair in Bitfinex_db_collection_names:
            for price_type in price_type_list:
                drop_repeated_data_queue.put({'symbol_pair': symbol_pair, 'price_type': price_type})
        return drop_repeated_data_queue

    def request_for_get_history_price_data(self, url):
        price_data = []
        price_type = url.split(':')[2]
        symbol_pair = url.split(':')[3].split('/')[0].split('t')[1].lower()
        end_timestamp = url.split(':')[3].split('=')[1].split('&')[0]
        symbol_pair_sheet = self.__db[symbol_pair]
        try:
            res = requests.get(url, headers=self.__base_headers)
            result = json.loads(res.text)
            for i in result:
                data = {'timestamp': i[0], 'open': i[1], 'close': i[2], 'high': i[3], 'low': i[4], 'volume': i[5],'price_type': price_type}
                price_data.append(data)
            next_timestamp = str(self.__create_next_end_timestamp(price_type, result[-1][0]))
            next_url = url.replace(end_timestamp, next_timestamp)
            if len(result) < 5000:
                print(symbol_pair, '的', price_type, 'k线类型历史数据已经爬取完毕')
            else:
                self.request_for_get_history_price_data(next_url)
            symbol_pair_sheet.insert_many(price_data)
        except requests.RequestException:
            self.request_for_get_history_price_data(url)

    def request_for_update_history_price_data(self, url):
        price_data = []
        price_type = url.split(':')[2]
        symbol_pair = url.split(':')[3].split('/')[0].split('t')[1].lower()
        end_timestamp = url.split(':')[3].split('=')[1].split('&')[0]
        symbol_pair_sheet = self.__db[symbol_pair]
        try:
            res = requests.get(url, headers=self.__base_headers)
            result = json.loads(res.text)
            for i in result:
                data = {'timestamp': i[0], 'open': i[1], 'close': i[2], 'high': i[3], 'low': i[4], 'volume': i[5],'price_type': price_type}
                price_data.append(data)
            next_timestamp = str(self.__create_next_end_timestamp(price_type, result[-1][0]))
            next_url = url.replace(end_timestamp, next_timestamp)
            if len(result) < 5000:
                need_update_price_data = []
                for data in price_data:
                    count = symbol_pair_sheet.count_documents(data)
                    if count == 0:
                        need_update_price_data.append(data)
                if len(need_update_price_data) == 0:
                    print(datetime.datetime.now(), symbol_pair, '的', price_type, 'k线数据已是最新数据')
                else:
                    print(datetime.datetime.now(), symbol_pair, '的', price_type, 'k线数据已经更新完毕')
                    symbol_pair_sheet.insert_many(need_update_price_data)
            else:
                need_update_price_data = []
                for data in price_data:
                    count = symbol_pair_sheet.count_documents(data)
                    if count == 0:
                        need_update_price_data.append(data)
                if len(need_update_price_data) == 0:
                    print(datetime.datetime.now(), symbol_pair, '的', price_type, 'k线数据已是最新数据')
                else:
                    print(datetime.datetime.now(), symbol_pair, '的', price_type, 'k线数据已经更新完毕')
                    symbol_pair_sheet.insert_many(need_update_price_data)
                self.request_for_update_history_price_data(next_url)
        except requests.RequestException:
            self.request_for_update_history_price_data(url)

    def check_symbol_pair_price_data_status(self):
        # 此函数作用是检查数据库中的每个交易对的状态，每个交易对的每个类型的k线数据量各有多少
        # 如果某个交易对的某个K线类型数量为0，则自动启动爬虫进行爬取
        now_time = int(time.time())*1000
        Bitfinex_db_collection_names = self.__db.list_collection_names()
        # Bitfinex_db_collection_names为数据库中已存在的交易对
        del Bitfinex_db_collection_names[Bitfinex_db_collection_names.index('symbol_detail')]
        for col_name in Bitfinex_db_collection_names:
            #symbol_pair_status = {}
            #symbol_pair_status['symbol_pair'] = col_name
            sheet = self.__db[col_name]
            for price_type in self.__price_type_list:
                price_type_len = sheet.count_documents({'price_type': price_type})
                #symbol_pair_status[price_type] = price_type_len
                if price_type_len == 0:
                    print('检查到', col_name, '的', price_type, 'k线类型为零，开始补全')
                    url = self.__history_price_url + price_type + ':' + 't' + col_name.upper() + '/hist?end=' + str(now_time) + '&limit=5000&_bfx=1'
                    self.request_for_get_history_price_data(url)
                    print(datetime.datetime.now(), col_name, '的', price_type, 'k线数据爬取完毕')
            #print(symbol_pair_status)

    def create_index_mongo(self):
        # 创建索引(by timestamp)，加快查询速度
        Bitfinex_db_collection_names = self.__db.list_collection_names()
        # Bitfinex_db_collection_names为数据库中已存在的交易对
        del Bitfinex_db_collection_names[Bitfinex_db_collection_names.index('symbol_detail')]
        for symbol_pair in Bitfinex_db_collection_names:
            sheet = self.__db[symbol_pair]
            sheet.create_index('timestamp')
            print(sheet.index_information())

    def show_db_the_last_update_times(self, symbol_pair=None, price_type=None):
        # 展示数据库中历史价格数据的最近更新时间
        if symbol_pair is None and price_type is None:
            Bitfinex_db_collection_names = self.__db.list_collection_names()
            del Bitfinex_db_collection_names[Bitfinex_db_collection_names.index('symbol_detail')]
            price_type_list = self.__price_type_list
            for symbol_pair in Bitfinex_db_collection_names:
                symbol_pair_sheet = self.__db[symbol_pair]
                for price_type in price_type_list:
                    the_last_update_timestamp = list(symbol_pair_sheet.find({'price_type': price_type}).sort('timestamp', pymongo.DESCENDING).limit(1))[0]['timestamp']/1000
                    dateArray = datetime.datetime.fromtimestamp(the_last_update_timestamp)
                    the_last_update_time = dateArray.strftime("%Y-%m-%d %H:%M:%S")
                    print(symbol_pair, '的', price_type, 'k线类型历史价格数据更新时间为', the_last_update_time)
        else:
            symbol_pair_sheet = self.__db[symbol_pair]
            the_last_update_timestamp = list(symbol_pair_sheet.find({'price_type': price_type}).sort('timestamp', pymongo.DESCENDING).limit(1))[0]['timestamp'] / 1000
            dateArray = datetime.datetime.fromtimestamp(the_last_update_timestamp)
            the_last_update_time = dateArray.strftime("%Y-%m-%d %H:%M:%S")
            print(symbol_pair, '的', price_type, 'k线类型历史价格数据更新时间为', the_last_update_time)

    def drop_repeated_data(self, symbol_pair, price_type):
        symbol_pair_sheet = self.__db[symbol_pair]
        price_type_price_data = list(symbol_pair_sheet.find({'price_type': price_type}))
        try:
            price_type_price_data_len = len(price_type_price_data)
            price_type_price_data_distinct_len = len(symbol_pair_sheet.find({'price_type': price_type}).distinct('timestamp'))
            if price_type_price_data_len == price_type_price_data_distinct_len:
                print(datetime.datetime.now(), symbol_pair, '的', price_type, '的k线历史价格数据已经去重完毕')
            else:
                for price_data in price_type_price_data:
                    data_timestamp = price_data['timestamp']
                    data_timestamp_count = symbol_pair_sheet.count_documents({'price_type': price_type, 'timestamp': data_timestamp})
                    while data_timestamp_count > 1:
                        # 当表中某一值存在大于1条数据时，则去重
                        symbol_pair_sheet.delete_one({'price_type': price_type, 'timestamp': data_timestamp})
                        # print(datetime.datetime.now(), symbol_pair, '的', price_type, '的', data_timestamp, '已删除重复值')
                        data_timestamp_count = symbol_pair_sheet.count_documents({'price_type': price_type, 'timestamp': data_timestamp})
                print(datetime.datetime.now(), symbol_pair, '的', price_type, '的k线历史价格数据已经去重完毕')
        except pymongo.errors.OperationFailure:
            for price_data in price_type_price_data:
                data_timestamp = price_data['timestamp']
                data_timestamp_count = symbol_pair_sheet.count_documents(
                    {'price_type': price_type, 'timestamp': data_timestamp})
                while data_timestamp_count > 1:
                    # 当表中某一值存在大于1条数据时，则去重
                    symbol_pair_sheet.delete_one({'price_type': price_type, 'timestamp': data_timestamp})
                    # print(datetime.datetime.now(), symbol_pair, '的', price_type, '的', data_timestamp, '已删除重复值')
                    data_timestamp_count = symbol_pair_sheet.count_documents(
                        {'price_type': price_type, 'timestamp': data_timestamp})
            print(datetime.datetime.now(), symbol_pair, '的', price_type, '的k线历史价格数据已经去重完毕')

