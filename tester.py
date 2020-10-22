"""
此模块是用来检测代理是否可用，若代理可用则将其分数置为100，若不可用则将其分数减1
aiohttp模块是一个异步的请求模块，效率比requests高很多，由于有大量的代理需要检测其可用性，因此使用aiohhtp模块
"""

import aiohttp
import asyncio
import redisClient

TEST_URL = "http://www.baidu.com"  # 测试的网址，建议写实际需要爬取的网址
VALID_STATUS_CODE = [200]  # 定义合法的响应状态码
BATCH_TEST_SIZE = 100  # 定义批量检测的代理的数量


class Tester:
    """
    此类是用来检测代理是否可用的类
    """

    def __init__(self):
        """
        初始化函数中生成RedisClient对象
        """
        self.redis = redisClient.RedisClient()

    async def test_single_proxy(self, proxy):
        """
        异步方法：测试单个代理是否可用，可用即将其分数置为100，否则减1
        :param proxy: 代理
        :return: None
        """
        conn = aiohttp.TCPConnector(verify_ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            try:
                if isinstance(proxy, bytes):
                    proxy = proxy.decode('utf-8')  # 若代理是字节数组类型则将其转为字符串类型
                real_proxy = 'http://' + proxy
                async with session.get(TEST_URL, proxy=real_proxy, timeout=15) as response:
                    if response.status in VALID_STATUS_CODE:
                        self.redis.max(proxy)
                    else:
                        self.redis.decrease(proxy)
            except Exception as e:
                self.redis.decrease(proxy)
                print(e)

    def run(self):
        """
        主函数
        :return: None
        """
        try:
            proxies = self.redis.all()
            loop = asyncio.get_event_loop()  # 获取轮训器
            for i in range(0, len(proxies), BATCH_TEST_SIZE):
                test_proxies = proxies[i, i + BATCH_TEST_SIZE]
                tasks = [self.test_single_proxy(proxy) for proxy in test_proxies]   # 将协程函数放入到列表，协程函数只能在轮训器中执行，所以此列表中放入的是协程函数，而不是协程函数返回的值
                loop.run_until_complete(asyncio.wait(tasks))    # 协程在轮训器中执行
        except Exception as e:
            print("测试代理发生错误", e)


if __name__ == '__main__':
    test = Tester()
    test.run()
