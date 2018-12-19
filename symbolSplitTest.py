# -*- coding: utf-8 -*-
"""
Created on Fri Nov 16 18:12:19 2018

@author: 18665
"""
from saveSymbolList import filename


def get_all_symbol_detail():
        import json
        with open(filename,'r') as f:
            symbolList = json.load(f)
        return symbolList

def symbol_replace(symbol):
    if 'usd' in symbol:
        return symbol.replace('usd','_usd')
    elif 'eth' in symbol  and 'btc' not in symbol:
        if 'usd' not in symbol:
            return symbol.replace('eth','_eth')
    elif 'btc' in symbol:
        if 'usd' not in symbol:
            return symbol.replace('btc','_btc')
        
if __name__ == '__main__':
    symbolList = get_all_symbol_detail()
    dicts = {}
    for s in symbolList:
        print(s)
        ss = symbol_replace(s)
        print(ss)
        dicts[s] = ss