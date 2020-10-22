"""
此模块是代理池的存储模块，用来进行redis相关的操作
"""

MAX_SCORE = 100  # 可用代理的最大分数
MIN_SCORE = 0  # 代理的最小分数
INITIAL_SCORE = 10  # 新代理的初始分数
REDIS_HOST = 'localhost'  # redis服务的主机
REDIS_PORT = 6379  # redis服务的端口，默认为6397
REDIS_PASSWORD = None  # redis的密码
REDIS_KEY = 'proxies'  # 自定义的用来存储代理的有序集合的键名

from random import choice
import redis


class RedisClient:
    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD):
        """
        初始化方法，用来创建redis连接
        :param host: redis服务主机ip
        :param port: redis服务端口
        :param password: 密码
        """
        self.db = redis.StrictRedis(host=host, port=port, password=password, decode_responses=True)

    def add(self, proxy, score=INITIAL_SCORE):
        """
        添加代理到有序集合，若该代理已存在则不添加
        :param proxy: 被添加的代理
        :param score: 分数
        :return: 添加代理的数量
        """
        if not self.db.zscore(REDIS_KEY, proxy):
            mapping = {proxy: score}
            print("加入代理成功：", proxy, score)
            return self.db.zadd(REDIS_KEY, mapping)

    def decrease(self, proxy):
        """
        代理值减一分，若该代理分数小于最小分数，则直接删除代理
        :param proxy: 代理
        :return: 修改后的代理分数
        """
        score = self.db.zscore(REDIS_KEY, proxy)
        if score and score > MIN_SCORE:
            return self.db.zincrby(REDIS_KEY, proxy, -1)
        else:
            return self.db.zrem(REDIS_KEY, proxy)

    def random(self):
        """
        随机返回一个可用的代理，优先返回最高分代理，若没有最高分代理，则按照排名获取，否则异常
        :return: 随机代理
        """
        result = self.db.zrangebyscore(REDIS_KEY, MAX_SCORE, MAX_SCORE)
        if result:
            return choice(result)
        else:
            result = self.db.zrevrange(REDIS_KEY, 0, 100)  # 表示降序取从有序集合中取100个元素
            if result:
                return choice(result)
            else:
                raise Exception('no such a proxy')

    def exist(self, proxy):
        """
        判断代理是否存在
        :param proxy: 代理
        :return: 是否存在
        """
        return not self.db.zscore(REDIS_KEY, proxy) is None

    def max(self, proxy):
        """
        将代理的分数设为最大值
        :param proxy:
        :return: 返回设置的分数值
        """
        mapping = {proxy: MAX_SCORE}
        return self.db.zadd(REDIS_KEY, mapping)

    def count(self):
        """
        统计有序集合的元素个数
        :return: 返回个数
        """
        return self.db.zcard(REDIS_KEY)

    def all(self):
        """
        获取全部代理
        :return: 全部代理列表
        """
        return self.db.zrevrangebyscore(REDIS_KEY, MAX_SCORE, MIN_SCORE)
