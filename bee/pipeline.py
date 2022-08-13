#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/13 12:00
# @Author  : Dominolu
# @File    : pipeline.py
# @Software: PyCharm
import bee.sqlorm

class Pipeline(object):

    @classmethod
    def create(cls):
        return cls()

    def open(self):
        pass

    def close(self):
        pass

    def process_item(self, item):
        return item
