import json
from tornado.web import RequestHandler, HTTPError, gen
import logging
import app.settings as st
from os_tornado.decorators.json_response import jsonify
import time
import tornado
from app.common.status_api import FetchStatus
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor


_logger = logging.getLogger('CrawlerStatusHandler')

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
