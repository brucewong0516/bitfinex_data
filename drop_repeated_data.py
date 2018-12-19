# 多线程去重 半个小时左右
# 比较吃电脑内存
if __name__ == '__main__':
    from Bitfinex_api2 import Bitfinex_api
    import threading
    drop_repeated_data_queue = Bitfinex_api().create_drop_repeated_data_queue()

    class thread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)

        def run(self):
            while not drop_repeated_data_queue.empty():
                drop_repeated_data = drop_repeated_data_queue.get()
                symbol_pair = drop_repeated_data['symbol_pair']
                price_type = drop_repeated_data['price_type']
                Bitfinex_api().drop_repeated_data(symbol_pair, price_type)
    threads = [thread() for i in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()