import json
from tornado.web import RequestHandler, HTTPError, gen
import logging
import app.settings as st
from os_tornado.decorators.json_response import jsonify
import time
import tornado

_logger = logging.getLogger('CrawlerStatusHandler')

@jsonify
class CrawlerStatusHandler(RequestHandler):
    def initialize(self, **kwargs):
        self._redis_extension = self.application.manager.get_extension('SchedulerRedisConn')
        self._batch_id = st.BATCH_ID
        if not self._batch_id:
            _logger.error('Batch_id is not configured!')
            raise NameError('no batch_id!')

    def get(self, *args, **kwargs):
        conn = self._redis_extension.get_conn()
        num = self.get_argument('num', 100)
        type = self.get_argument('type', 'fail')
        batch_id = self.get_argument('batch_id', self._batch_id)
        if type == 'fail':
            key = 'fetch_fail_status:%s' % batch_id
        elif type == 'success':
            key = 'fetch_success_status:%s' % batch_id
        else:
            _logger.error('the type not supported!')
            raise HTTPError(403, reason='invalid type!')
        status = conn.hgetall(key)
        _logger.info('get data from redis: [%s]' % key)
        self.write(json.dumps(status))


@jsonify
class StatusCountHandler(RequestHandler):
    def initialize(self, **kwargs):
        self._redis_extension = self.application.manager.get_extension('SchedulerRedisConn')
        self._batch_id = st.BATCH_ID
        if not self._batch_id:
            _logger.error('batch_id is not configured!')
            raise NameError('no batch_id!')
        # self._fail_hash_name = 'fetch_fail_status:%s' % self._batch_id
        # self._succ_hash_name = 'fetch_success_status:%s' % self._batch_id

    def get(self, *args, **kwargs):
        period = self.get_argument('period', 'B')  # B for batch_id, D for day, H for hour,5M for five minites
        batch_id = self.get_argument('batch_id', self._batch_id)
        self._fail_hash_name = 'fetch_fail_status:%s' % self._batch_id
        self._succ_hash_name = 'fetch_success_status:%s' % self._batch_id
        _redis_conn = self._redis_extension.get_conn()
        if period == 'D':
            interval = 24*60*60
        elif period == 'H':
            interval = 60*60
        elif period == '5M':
            interval = 5*60
        elif period == 'B':
            succ_num = _redis_conn.hlen(self._succ_hash_name)
            fail_num = _redis_conn.hlen(self._fail_hash_name)
            return {'success num': succ_num, 'fail num': fail_num}
        else:
            _logger.error('invalid period argument : %s' % self.request)
            raise HTTPError(404, reason='invalid period!')
        succs = _redis_conn.hgetall(self._succ_hash_name)
        fails = _redis_conn.hgetall(self._fail_hash_name)
        cur_time = time.time()
        succ_num = 0
        fail_num = 0
        for k, v in succs.items():
            if int(json.loads(v)['status_time']) + interval > cur_time:
                succ_num += 1
        for k, v in fails.items():
            t = json.loads(v)['status_time']
            if float(t) + interval > cur_time:
                fail_num += 1
        return {'success num': succ_num, 'fail num': fail_num}

from app.common.status_api import FetchStatus
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor


@jsonify
class StatusCheckHandler(RequestHandler):

    def initialize(self, **kwargs):
        self._redis_extension = self.application.manager.get_extension('SchedulerRedisConn')
        self._batch_id = st.BATCH_ID
        if not self._batch_id:
            _logger.error('Batch_id is not configured!')
            raise NameError('No batch_id!')

    @tornado.web.asynchronous
    @gen.coroutine
    # def get(self, url, batch_id):
    def post(self, *args, **kwargs):
        url = self.get_argument('url')
        _redis_conn = self._redis_extension.get_conn()
        _batch_id = self.get_argument('batch_id', self._batch_id)
        if not url:
            _logger.error('Invalid url argument : %s' % self.request)
            raise HTTPError(404, reason='Invalid url!')
        else:
            # status = yield gen.maybe_future(FetchStatus(_redis_conn, self._batch_id).get_fetch_status(url))
            status = yield FetchStatus(_redis_conn, _batch_id).get_fetch_status(url)
            if not status:
                _logger.warning('No status for url: %s' % self.request)
                self.write({'status': '-1', 'url': url})
                self.finish()
            else:
                self.write(json.dumps(status))
                self.finish()
