#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/6 11:33
# @Author  : Dominolu
# @File    : tools.py
# @Software: PyCharm
import socket
import uuid
import psutil as ps
from datetime import datetime

def get_hostname():
    socket.gethostname()

def get_host_ip():
    """
    查询本机ip地址
    :return: ip
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

def get_network_info():
        conns = ps.net_connections()
        count = 0
        for conn in conns:
            if conn.status is 'ESTABLISHED':
                count = count + 1
        return {'count':len(conns),'established':count}


def get_cpu_info():
    cpu={}
    cpu['cores'] = ps.cpu_count()
    cpu['percent'] = ps.cpu_percent(interval=2)
    cpu_times = ps.cpu_times()
    cpu['system_time'] = cpu_times.system
    cpu['user_time'] = cpu_times.user
    cpu['idle_time'] = cpu_times.idle
    cpu['nice_time'] = cpu_times.nice
    cpu['softirq'] = cpu_times.softirq
    cpu['irq'] = cpu_times.irq
    cpu['iowait'] = cpu_times.iowait
    return cpu

def get_timestamp():
    return int((datetime.timestamp(datetime.now())))

def get_mem_info(self):
    mem_info = ps.virtual_memory()
    self.mem['percent'] = mem_info.percent
    self.mem['total'] = mem_info.total
    self.mem['vailable'] = mem_info.available
    self.mem['used'] = mem_info.used
    self.mem['free'] = mem_info.free
    self.mem['active'] = mem_info.active

def get_mac_address():
    mac = uuid.UUID(int=uuid.getnode()).hex[-12:]
    MAC = ":".join([mac[e: e + 2] for e in range(0, 11, 2)])
    return MAC