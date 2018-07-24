# coding: utf-8
from tornado import gen, httpclient, ioloop
import app.settings as st
import logging
import json
import xmlrpclib
import time
from retrying import retry
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor

_logger = logging.getLogger('UrlSender')

class UrlSender(object):

    executor = ThreadPoolExecutor(10)

    def __init__(self, redis_conn, batch_id, master_server):
        self._redis_conn = redis_conn
        self._batch_id = batch_id
        if not self._batch_id:
            _logger.error('there is no batch_id!')
            raise NameError('there is no batch_id!')
        self._master_server = master_server

    @retry(stop_max_attempt_number=3, wait_fixed=1000)
    @run_on_executor
    def send_url_to_master(self, url, meta, priority=0):
        # meta['batch_id'] = self._batch_id
        request_json = {
            "url": url.strip(),
            "meta": meta,
            "priority": priority,
        }
        if meta.get('User-Agent', None):
            request_json['headers'] = {'User-Agent': meta['User-Agent']}
        ret = 1
        try:
            seed = json.dumps(request_json)
            server = xmlrpclib.ServerProxy(self._master_server)
            ret = server.api.send.sender.send(seed)
            # if not str(ret) == '0':
            #     raise Exception('send fail: <%s> [%s]' % (url, str(ret)))
        except Exception as e:
            _logger.exception('send fail: <%s> [%s]' % (url, e.message))
        if not str(ret) == '0':
            _logger.error('send fail: <%s> [%s]' % (url, str(ret)))
        else:
            _logger.info('send success: <%s> [%s]' % (url, str(ret)))
        return ret