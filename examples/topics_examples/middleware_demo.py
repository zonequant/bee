#!/usr/bin/env python

from ruia import Spider, Middleware

middleware = Middleware()


@middleware.request
async def print_on_request(request):
    request.metadata = {
        'url': request.url
    }
    print(f"request: {request.metadata}")
    # Just operate request object, and do not return anything.


@middleware.response
async def print_on_response(request, response):
    print(f"response: {response.metadata}")


class MiddlewareSpiderDemo(Spider):
    start_urls = ['http://www.httpbin.org/get']
    concurrency = 10

    async def parse(self, response):
        pages = [f'http://www.httpbin.org/get?p={i}' for i in range(1, 3)]
        async for resp in self.multiple_request(urls=pages):
            print(resp.url)


if __name__ == '__main__':
    MiddlewareSpiderDemo.start(middleware=middleware)