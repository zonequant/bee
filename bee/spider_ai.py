#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/8/10 23:03
# @Author  : Dominolu
# @File    : spider_ai.py
# @Software: PyCharm

class Spider(object):
    def __init__(self):
        pass
    async def _run_spider_hook(self, hook_func):
        """
        Run hook before/after spider start crawling
        :param hook_func: aws function
        :return:
        """
        if callable(hook_func):
            try:
                aws_hook_func = hook_func(weakref.proxy(self)) # 创建弱引用
                if isawaitable(aws_hook_func):          # 如果对象isawait可返回True
                    await aws_hook_func
            except Exception as e:
                raise SpiderHookError(f"<Hook {hook_func.__name__}: {e}")

    async def process_failed_response(self, request, response):
        """     失败响应的处理
        Corresponding processing for the failed response
        :param request: Request
        :param response: Response
        :return:
        """
        pass

    async def process_succeed_response(self, request, response):
        """     成功响应的处理
        Corresponding processing for the succeed response
        :param request: Request
        :param response: Response
        :return:
        """
        pass

    async def process_item(self, item,spider):
        """
        Corresponding processing for the Item type
        """
        pass


    async def process_callback_result(self, callback_result):
        """
        Corresponding processing for the invalid callback result
        :param callback_result: Custom instance
        :return:
        """
        callback_result_name = type(callback_result).__name__
        process_func_name = self.callback_result_map.get(callback_result_name, "")
        process_func = getattr(self, process_func_name, None)
        if process_func is not None:
            await process_func(callback_result)
        else:
            raise InvalidCallbackResult(
                f"<Parse invalid callback result type: {callback_result_name}>"
            )
