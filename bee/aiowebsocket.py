#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/9/4 00:39
# @Author  : Dominolu
# @File    : aiowebsocket.py
# @Software: PyCharm
import aiohttp
from asyncio import (
    get_event_loop,
    set_event_loop,
    AbstractEventLoop,
    run_coroutine_threadsafe,
    sleep
)
from threading import Thread
from aiohttp.http import WSMsgType
import json
import traceback
import threading
from loguru import logger as log

class Async_loop():
    _instance_lock = threading.Lock()
    _loop=None

    def __new__(cls, *args, **kwargs):
        if not hasattr(Async_loop, "_instance"):
            with Async_loop._instance_lock:
                if not hasattr(Async_loop, "_instance"):
                    Async_loop._instance = object.__new__(cls)
                    Async_loop._instance._loop=get_event_loop()
                    Async_loop._instance.start()
        return Async_loop._instance

    def start(self):
        thread = Thread(target=self.run_loop, args=(self._loop,))
        thread.daemon = True
        thread.start()
        log.debug("loop started")

    def run_loop(self,loop):
        set_event_loop(loop)
        self._loop.run_forever()

    def create_task(self,coro):
        run_coroutine_threadsafe(coro, self._loop)

class Websocket():
    url = ""
    connected_callback = None
    process_callback = None
    process_binary_callback = None
    check_conn_interval = None

    def __init__(self, check_conn_interval=10, proxy=None):
        """Initialize."""
        self.check_conn_interval = check_conn_interval
        self.proxy = proxy
        self._ws = None  # Websocket connection object.
        self._loop=Async_loop()

    @property
    def ws(self):
        return self._ws

    def init(self, url, connected_callback, process_callback=None, process_binary_callback=None):
        self.url = url
        self.connected_callback = connected_callback
        self.process_binary_callback = process_binary_callback
        self.process_callback = process_callback

    async def conn(self):
        await self._connect()

    async def _connect(self):
        session = aiohttp.ClientSession()
        try:
            if self.proxy:
                self._ws = await session.ws_connect(self.url, proxy=self.proxy)
            else:
                self._ws = await session.ws_connect(self.url)
                log.debug("ws connected")
            await self.connected_callback()
            log.debug("ws connected callback")
            await self._receive()
            await self.on_disconnected()
        except Exception:
            print("ws error")
            traceback.print_exc()
            await self._check_connection()


    async def on_disconnected(self):
        log.info("connected closed.")
        await self._check_connection()

    async def _reconnect(self):
        """Re-connect to Websocket server."""
        log.warning("reconnecting to Websocket server right now!")
        await self._connect()

    async def _receive(self):
        """Receive stream message from Websocket connection."""
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                if self.process_callback:
                    try:
                        data = json.loads(msg.data)
                    except:
                        data = msg.data
                    await self.process_callback(data)
            elif msg.type == aiohttp.WSMsgType.BINARY:
                if self.process_binary_callback:
                    await self.process_binary_callback(msg.data)
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                log.warning("receive event CLOSED:", msg, caller=self)
                await self._reconnect()
            elif msg.type == aiohttp.WSMsgType.PING:
                await self._ws.send(WSMsgType.PONG)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                log.error("receive event ERROR:", msg, caller=self)
            else:
                log.warning("unhandled msg:", msg, caller=self)

    async def _check_connection(self):
        """Check Websocket connection, if connection closed, re-connect immediately."""
        if not self._ws:
            log.warning("Websocket connection not connected yet!")
            return
        if self._ws.closed:
            await self._reconnect()

    async def send(self, data):
        try:
            if not self.ws:
                log.warning("Websocket connection not connected yet!")
                return False
            if isinstance(data, dict):
                await self.ws.send_json(data)
            elif isinstance(data, str):
                await self.ws.send_str(data)
            else:
                return False
            return True
        except :
            log.error("ws send error")
            await self._check_connection()
