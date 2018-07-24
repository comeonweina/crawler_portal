# coding: utf-8
import json
from tornado.web import RequestHandler,HTTPError
import logging
import app.settings as st
from os_tornado.decorators.json_response import jsonify
import time
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor

_logger = logging.getLogger('FetchStatus')


class FetchStatus(object):

    executor = ThreadPoolExecutor(10)

    def __init__(self, redis_conn, batch_id):
        self._redis_conn = redis_conn
        self._batch_id = batch_id
        if not self._batch_id:
            _logger.error('batch_id is not configured!')
            raise NameError('no batch_id!')
        self._fail_hash_name = 'fetch_fail_status:%s' % self._batch_id
        self._succ_hash_name = 'fetch_success_status:%s' % self._batch_id

    @run_on_executor
    def get_fetch_status(self, url):
        if not url:
            _logger.error('invalid url!')
            # raise HTTPError(404, reason='invalid url!')
            return None
        else:
            succ = self._redis_conn.hget(self._succ_hash_name, url)
            fail = self._redis_conn.hget(self._fail_hash_name, url)
            if succ:
                return json.loads(succ)
            elif fail:
                return json.loads(fail)
            else:
                _logger.warning('no status for url: %s' % url)
                return None

    def get_fetch_status_with_time(self, url, timeout):
        _start = time.time()
        status = None
        while time.time()-_start < timeout:
            status = self.get_fetch_status(url)
            if status:
                return status
            time.sleep(timeout/10)  #try 10 times
        return status