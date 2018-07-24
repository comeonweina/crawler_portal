import redis
import logging
from redis import StrictRedis
from os_tornado.extension import Extension


class RedisExtension(Extension):

    def setup(self):
        redis_address = self.ext_settings['redis_address']
        self._conn = StrictRedis.from_url(redis_address)

    def get_conn(self):
        return self._conn
