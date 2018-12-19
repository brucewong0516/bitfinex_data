# -*- coding: utf-8 -*-
"""
Created on Fri Nov 16 15:14:16 2018

@author: 18665
"""
import os
import requests
import json

headers = {
            'referer': 'https://www.bitfinex.com/',
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'
                }
url = 'https://api.bitfinex.com/v1/symbols_details'
filename = os.path.dirname(os.path.abspath(__file__)) + '\symbolList.json'


if __name__ == '__main__':
    res = requests.get(url=url, headers=headers)
    infos = json.loads(res.text)
    symbolList = []
    for info in infos:
        symbolList.append(info['pair'])
    with open(filename,'w') as f:
        json.dump(symbolList,f) 