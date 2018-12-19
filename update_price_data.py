# 若中断了直接再次运行即可
if __name__ == '__main__':
    from Bitfinex_api2 import Bitfinex_api
    import threading
    initially_update_urls_queue = Bitfinex_api().create_initially_update_urls_queue()

    class thread(threading.Thread):
        def __init__(self):
            threading.Thread.__init__(self)

        def run(self):
            while not initially_update_urls_queue.empty():
                Bitfinex_api().request_for_update_history_price_data(initially_update_urls_queue.get())


    threads = [thread() for i in range(50)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()