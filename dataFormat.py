# -*- coding: utf-8 -*-
"""
Created on Fri Nov 16 16:05:34 2018

@author: 18665
"""
class data(object):
    
    def __init__(self):
        self.exchange = ''
        self.symbol = ''
        self.frequency = ''
        self.start_time = None
        self.end_time = None
        self.open = 0.0
        self.high = 0.0
        self.low = 0.0
        self.close = 0.0
        self.volume = 0.0
        self.amount = 0.0
        self.openInterest = 0.0