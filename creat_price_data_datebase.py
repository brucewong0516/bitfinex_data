# 若中断了直接再次运行即可
# 爬取历史数据建立数据库
if __name__ == '__main__':
    from crypto_1min import Bitfinex_api
    import threading
    initially_urls_queue = Bitfinex_api().create_initially_urls_queue()
#    Bitfinex_api().get_all_symbol_detail()

    class thread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)

        def run(self):
            while not initially_urls_queue.empty():
                Bitfinex_api().request_for_get_history_price_data(initially_urls_queue.get())

    threads = [thread() for i in range(50)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()