#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/4 00:30
# @Author  : Dominolu
# @File    : liquidation.py
# @Software: PyChar
from bee.aiowebsocket import Websocket

class Bybit_liqudation(Websocket):
    url="wss://stream.bybit.com/realtime"
    subscribe='{"op":"subscribe","args":["liquidation"]}'

    def __init__(self):
        super(Bybit_liqudation, self).__init__()

    def start(self):
        """
        """
        self.init(self.url, self.connected_callback, self.process_callback, self.process_binary_callback)
        self._loop.create_task(self._connect())
        self.feed()

    def process_callback(self,data):
        print(data)

    def feed(self):
        self._loop.create_task(self.send(self.subscribe))

