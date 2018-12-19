# 单线程Bitfinex历史价格数据爬虫（稳定但速度较慢）
# 爬取全部的价格数据大概需要4-5小时
# 已连接数据库（MongoDB）自动储存
# 更新全部价格数据大概需要30分钟
import requests
import json
import time
import pymongo
import datetime


class Bitfinex_api(object):

    def __init__(self, client_url='localhost'):
        self.__base_headers = {
            'referer': 'https://www.bitfinex.com/',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
        }
        self.__params = {
            'end': '',
            'limit': '',
            '_bfx': 1
        }
        self.__symbol_detail_url = 'https://api.bitfinex.com/v1/symbols_details'
        self.__history_price_url = 'https://www.bitfinex.com/v2/candles/trade:'
        self.__client = pymongo.MongoClient(client_url, 27017)
        self.__db = self.__client['Bitfinex_db']
        self.price_type_list = ['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D']

    def __creat_next_end_timestamp(self, price_type, end_timestamp):
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
            print('数据库中已存在symbol_detail表')
        else:
            # 获取所有的交易对的详细信息
            try:
                res = requests.get(url=self.__symbol_detail_url, headers=self.__base_headers)
                symbol_detail_data = json.loads(res.text)
                symbol_detail_sheet = self.__db['symbol_detail']
                symbol_detail_sheet.insert_many(symbol_detail_data)
                print('已建立symbol_detail表')
                return symbol_detail_data
            except requests.RequestException:
                print('获取symbol_detail时网络出现错误！')

    def get_one_symbol_pair_history_price(self, params_dict):
        # price_type 为k线类型，可选参数['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D']
        # symbol_pair为交易对名称， 具体交易对名称可由上个函数的返回值symbol_detail_data查看键值pair
        # 返回值为某k线类型的所有历史价格数据，为[{},{},{},{},{},{}]形式的列表
        price_type = params_dict['price_type']
        symbol_pair = params_dict['symbol_pair']
        end_timestamp = int(time.time()*1000)
        # 获取现在时间，作为end参数
        history_price_url = self.__history_price_url+price_type+':'+'t'+symbol_pair.upper()+'/hist'
        flag = 1
        # 做标记
        all_history_price_data = []
        # 用来储存历史价格数据
        while flag == 1:
            params = self.__params
            params['end'] = end_timestamp
            # 第一次参数使用现在的毫秒级时间戳
            params['limit'] = 5000
            # 一个页面5000条 最大5000条
            res = requests.get(url=history_price_url, headers=self.__base_headers, params=params)
            history_price_data = json.loads(res.text)
            for i in history_price_data:
                price_data = {'timestamp': i[0], 'open': i[1], 'close': i[2], 'high': i[3], 'low': i[4], 'volume': i[5], 'price_type': price_type}
                all_history_price_data.append(price_data)
            if len(history_price_data) < 5000:
                # 当len(history_price_data)小于5000的时候，证明数据已经到底
                flag = 0
                # 跳出循环while循环，否则继续构建下个end时间戳
            end_timestamp = self.__creat_next_end_timestamp(price_type, history_price_data[-1][0])
            # 再次构建下个end时间戳
        return all_history_price_data

    def get_all_symbol_pair_price_data(self):
        # 此函数的作用是获得数据库内所有交易对所有k线类型的历史价格数据，并储存到名为Bitfinex_db的数据库中
        symbol_detail_sheet = self.__db['symbol_detail']
        total_symbol_pairs = []
        for i in symbol_detail_sheet.find({}, {'pair': True, '_id': False}):
            total_symbol_pairs.append(i['pair'])
        # total_symbol_pairs为Bitfinex中所拥有的所有的交易对列表
        Bitfinex_db_collection_names = self.__db.list_collection_names()
        # Bitfinex_db_collection_names为数据库中已存在的交易对
        del Bitfinex_db_collection_names[Bitfinex_db_collection_names.index('symbol_detail')]
        for col_name in Bitfinex_db_collection_names:
            if col_name in total_symbol_pairs:
                del total_symbol_pairs[total_symbol_pairs.index(col_name)]
        # 得到的 total_symbol_pairs 为未爬取的交易对列表
        params_list = []
        for symbol_pair in total_symbol_pairs:
            for price_type in self.price_type_list:
                params_list.append({'price_type': price_type, 'symbol_pair': symbol_pair})
        # 构建参数字典，得到 params_list
        for i in params_list:
        # 开始爬取
            symbol_pair = i['symbol_pair']
            price_type = i['price_type']
            symbol_pair_sheet = self.__db[symbol_pair]
            all_history_price_data = self.get_one_symbol_pair_history_price(i)
            symbol_pair_sheet.insert_many(all_history_price_data)
        # 存进数据库
            print(datetime.datetime.now(), symbol_pair, '的', price_type, 'k线数据爬取完毕')
            time.sleep(0.1)
        print('所有交易对的历史数据爬取完毕  = .=')

    def update_one_symbol_pair_price(self, params_dict):
        # 此函数的作用是更新一个交易对的历史价格数据
        # prcie_type为k线类型，symbol_pair为交易对名称，db_end_timestamp为数据库中某类型k线历史价格的最新时间戳
        # 返回类似[{},{},{},{},{},{}]形式的列表
        symbol_pair = params_dict['symbol_pair']
        price_type = params_dict['price_type']
        symbol_pair_sheet = self.__db[symbol_pair]
        # 连接数据库
        db_end_timestamp = list(symbol_pair_sheet.find({'price_type': price_type}).sort('timestamp', pymongo.DESCENDING).limit(1))[0]['timestamp']
        # 以timestamp为单位，按降序排行，获取第一个timestamp，即数据库中最新的历史价格数据的时间
        history_price_url = self.__history_price_url+price_type+':'+'t'+symbol_pair.upper()+'/hist'
        price_type_list = ['1m', '5m', '15m', '30m', '1h', '3h', '6h', '12h', '1D']
        price_type_limit = [60000, 300000, 900000, 1800000, 3600000, 10800000, 21600000, 43200000, 86400000]
        limit = int((time.time() * 1000 - db_end_timestamp) / price_type_limit[price_type_list.index(price_type)])-1
        # (now_time-db_end_timestamp)/时间间隔，计算出另外一个参数limit的值
        all_update_price_data = []
        # 用来储存更新的数据
        end_timestamp = int(time.time() * 1000)
        # 第一次使用的参数
        try:
            if limit >= 1:
                # limit大于1证明有大于一个数据没有更新
                flag = 1
                # 标记flag=1，进入while循环
            else:
                flag = 0
                # 标记flag=0，不进入while循环
            while flag == 1:
                if limit >= 5000:
                    # limit最大值只能为5000
                    a = 5000
                else:
                    a = limit
                params = self.__params
                params['end'] = end_timestamp
                params['limit'] = a
                res = requests.get(url=history_price_url, headers=self.__base_headers, params=params)
                update_price_data = json.loads(res.text)
                for i in update_price_data:
                    price_data = {'timestamp': i[0], 'open': i[1], 'close': i[2], 'high': i[3], 'low': i[4], 'volume': i[5], 'price_type': price_type}
                    all_update_price_data.append(price_data)
                    # 整理数据
                if len(update_price_data) < 5000:
                    # len(update_price_data)<5000，更新的数据已经到底
                    flag = 0
                    # 标记flag=0，跳出while循环
                end_timestamp = self.__creat_next_end_timestamp(price_type, update_price_data[-1][0])
                # 构造下一个参数end
                limit = limit-5000
                # 构造下一个参数limit
            # ————————————————————————————————————————————————————
            # 下面的代码作用是检测返回的数据数据是否为最新数据
            need_update_price_data = []
            for data in all_update_price_data:
                count = symbol_pair_sheet.count_documents(data)
                if count == 0:
                    need_update_price_data.append(data)
            if len(need_update_price_data) == 0:
                print(datetime.datetime.now(), symbol_pair, '的', price_type, 'k线数据已是最新数据')
            else:
                print(datetime.datetime.now(), symbol_pair, '的', price_type, 'k线数据已经更新完毕')
                symbol_pair_sheet.insert_many(need_update_price_data)
        except requests.RequestException:
            print(datetime.datetime.now(), symbol_pair, '的', price_type, '网络错误')
            self.update_one_symbol_pair_price(params_dict)

    def update_all_symbol_pair_price_data(self):
        # 此函数的作用是更新所有交易对所有k线类型的历史价格数据，并储存到名为Bitfinex_db的数据库中
        Bitfinex_db_collection_names = self.__db.list_collection_names()
        del Bitfinex_db_collection_names[Bitfinex_db_collection_names.index('symbol_detail')]
        price_type_list = self.price_type_list
        params_list = []
        for symbol_pair in Bitfinex_db_collection_names:
            for price_type in price_type_list:
                params_dict = {'symbol_pair': symbol_pair, 'price_type': price_type}
                params_list.append(params_dict)
        for params_dict in params_list:
            self.update_one_symbol_pair_price(params_dict)
        print('所有交易对的历史数据更新完毕  = .=')

    def check_symbol_pair_price_data_status(self):
        # 此函数作用是检查数据库中的每个交易对的状态，每个交易对的每个类型的k线数据量各有多少
        # 如果某个交易对的某个K线类型数量为0，则自动启动爬虫进行爬取
        Bitfinex_db_collection_names = self.__db.list_collection_names()
        # Bitfinex_db_collection_names为数据库中已存在的交易对
        del Bitfinex_db_collection_names[Bitfinex_db_collection_names.index('symbol_detail')]
        for col_name in Bitfinex_db_collection_names:
            symbol_pair_status = {}
            symbol_pair_status['symbol_pair'] = col_name
            sheet = self.__db[col_name]
            for price_type in self.price_type_list:
                price_type_len = sheet.count_documents({'price_type': price_type})
                symbol_pair_status[price_type] = price_type_len
                if price_type_len == 0:
                    print('检查到', col_name, '的', price_type, 'k线类型为零，开始补全')
                    params_dict = {'price_type': price_type, 'symbol_pair': col_name}
                    all_history_price_data = self.get_one_symbol_pair_history_price(params_dict)
                    sheet.insert_many(all_history_price_data)
                    print(datetime.datetime.now(), col_name, '的', price_type, 'k线数据爬取完毕')
            print(symbol_pair_status)

    def creat_index_mongo(self):
        # 创建索引(by timestamp)，加快查询速度
        Bitfinex_db_collection_names = self.__db.list_collection_names()
        # Bitfinex_db_collection_names为数据库中已存在的交易对
        del Bitfinex_db_collection_names[Bitfinex_db_collection_names.index('symbol_detail')]
        for symbol_pair in Bitfinex_db_collection_names:
            sheet = self.__db[symbol_pair]
            sheet.create_index('timestamp')
            print(sheet.index_information())

    def drop_repeated_data(self):
        # 数据去重，最好在建立了索引的情况下运行，需要时间较长
        Bitfinex_db_collection_names = self.__db.list_collection_names()
        price_type_list = self.price_type_list
        del Bitfinex_db_collection_names[Bitfinex_db_collection_names.index('symbol_detail')]
        # Bitfinex_db_collection_names为数据库中已存在的交易对
        drop_repeated_data_db = self.__client['drop_repeated_data']
        # 建立临时数据库，去重以后自动删除
        drop_repeated_data_sheet = drop_repeated_data_db['drop_repeated_data_sheet']
        # 建立临时表，去重以后自动删除
        sheet_symbol_pairs = [x['symbol_pair'] for x in list(drop_repeated_data_sheet.find())]
        # 表中的已去重交易对列表
        for i in sheet_symbol_pairs:
            if i in Bitfinex_db_collection_names:
                del Bitfinex_db_collection_names[Bitfinex_db_collection_names.index(i)]
        # 去掉已经去重的交易对，类似断点重续，不执行到最后一步，数据库仍然存在
        for symbol_pair in Bitfinex_db_collection_names:
            symbol_pair_sheet = self.__db[symbol_pair]
            for price_type in price_type_list:
                price_type_data = list(symbol_pair_sheet.find({'price_type': price_type}))
                for data in price_type_data:
                    data_timestamp = data['timestamp']
                    data_timestamp_count = symbol_pair_sheet.count_documents({'price_type': price_type, 'timestamp': data_timestamp})
                    while data_timestamp_count > 1:
                        # 当表中某一值存在大于1条数据时，则去重
                        # print(datetime.datetime.now(), symbol_pair, '的', price_type, '的', data_timestamp, '有重复值')
                        symbol_pair_sheet.delete_one({'price_type': price_type, 'timestamp': data_timestamp})
                        print(datetime.datetime.now(), symbol_pair, '的', price_type, '的', data_timestamp, '已删除重复值')
                        data_timestamp_count = symbol_pair_sheet.count({'price_type': price_type, 'timestamp': data_timestamp})
                print(datetime.datetime.now(), symbol_pair, '的', price_type, '的k线历史价格数据已经去重完毕')
            drop_repeated_data_sheet.insert_one({'symbol_pair': symbol_pair})
            # 完成一个交易对的去重以后加入到临时数据库中
        print(datetime.datetime.now(), '所有历史价格数据去重完毕=。=')
        self.__client.drop_database('drop_repeated_data')
        # 删除临时数据库

    def show_db_the_last_update_times(self, symbol_pair=None, price_type=None):
        # 展示数据库中历史价格数据的最近更新时间
        if symbol_pair is None and price_type is None:
            Bitfinex_db_collection_names = self.__db.list_collection_names()
            del Bitfinex_db_collection_names[Bitfinex_db_collection_names.index('symbol_detail')]
            price_type_list = self.price_type_list
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


