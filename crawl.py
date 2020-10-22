"""
此模块是获取模块，主要是用来在各大网站抓取代理
"""

from lxml import etree
import time
from utils import get_page


class ProxyMetaclass(type):
    """
    自定义的元类，主要是将继承自此类的类中所有以'crawl_'开头的方法放入到'__CrawlFunc__'属性中，
    并统计以'Crawl_'开头的方法的个数，并放入到'__CrawlFuncCount__'属性中
    """

    def __new__(cls, name, bases, attrs):
        count = 0
        attrs['__CrawlFunc__'] = []
        for k, v in attrs.items():
            if 'crawl_' in k:
                count += 1
                attrs['__CrawlFunc__'].append(k)
        attrs['__CrawlFuncCount__'] = count
        return type.__new__(cls, name, bases, attrs)


class Crawler(metaclass=ProxyMetaclass):
    """
    爬取代理的类，继承自定义的元类
    此类中每一个以crawl_开头的方法代表爬取一个代理网站，以ip:port的格式返回爬取的代理
    """

    def get_proxies(self, callback):
        """
        爬取代理
        :param callback: 回调函数名（即以crawl_开头的方法名）
        :return: 返回包含代理字符串的列表
        """
        proxies = []
        for proxy in eval('self.{}()'.format(callback)):
            proxies.append(proxy)
        return proxies

    def crawl_daili66(self, end_page=5):
        """
        爬取代理66
        :param end_page: 爬取的页数，默认5页
        :return: 代理
        """
        base_url = 'http://www.66ip.cn/{}.html'
        urls = [base_url.format(i) for i in range(1, end_page + 1)]
        for url in urls:
            print("Crawling", url)
            html = get_page(url)
            if html:
                parseHtml = etree.HTML(html)
                trs = parseHtml.xpath("//div[@align='center']/table//tr")[1:]
                for tr in trs:
                    ip = tr.xpath('./td[1]/text()')[0]
                    port = tr.xpath('./td[2]/text()')[0]
                    yield ":".join((str(ip), str(port)))

    def crawl_kuaidaili(self, end_page=5):
        """
        爬取快代理
        :param end_page: 爬取的页数，默认5页
        :return: 代理
        """
        base_url = 'https://www.kuaidaili.com/free/inha/{}'
        urls = [base_url.format(page) for page in range(1, end_page + 1)]
        for url in urls:
            print("Crawling", url)
            time.sleep(2)
            html = get_page(url)
            if html:
                parseHtml = etree.HTML(html)
                trs = parseHtml.xpath("//div[@id='list']/table/tbody/tr")
                for tr in trs:
                    ip = tr.xpath('./td[1]/text()')[0]
                    port = tr.xpath('./td[2]/text()')[0]
                    yield ":".join((str(ip), str(port)))


from redisClient import RedisClient

POOL_MAX_PROXY = 1000  # 最大的代理存储数量


class Getter:
    """
    此类是用来动态的调用所有以crawl_开头的方法，然后获取抓取到的代理，并存入到数据库
    """

    def __init__(self):
        self.crawl = Crawler()
        self.redis = RedisClient()

    def is_over_proxy(self):
        """
        判断是否达到了设置的最大代理存储数量
        :return: 布尔值
        """
        if self.redis.count() > POOL_MAX_PROXY:
            return True
        else:
            return False

    def run(self):
        """
        调用所有以crawl_开头的方法，然后获取抓取到的代理，并存入到数据库
        :return:
        """
        if not self.is_over_proxy():
            for callback_label in range(self.crawl.__CrawlFuncCount__):
                callback = self.crawl.__CrawlFunc__[callback_label]
                proxies = self.crawl.get_proxies(callback)
                for proxy in proxies:
                    self.redis.add(proxy)


if __name__ == '__main__':
    getter = Getter()
    getter.run()
