import json
from tornado.web import RequestHandler, HTTPError, gen
import logging
import app.settings as st
from os_tornado.decorators.json_response import jsonify
import tornado
from urllib import unquote
from app.common.sender import UrlSender
import time


_logger = logging.getLogger('CrawlHandler')

@jsonify
class CrawlHandler(RequestHandler):


    def initialize(self, **kwargs):
        self._redis_extension = self.application.manager.get_extension('SchedulerRedisConn')
        self._batch_id = st.BATCH_ID
        if not self._batch_id:
            _logger.error('Batch_id is not configured!')
            raise NameError('no batch_id!')
        self._master_server = st.MASTER_SERVER
        self._meta = kwargs['master_meta']
        self._priority = kwargs['priority']
        self._send_status_redis = st.SEND_STATUS_REDIS

    @tornado.web.asynchronous
    @gen.coroutine
    def post(self, *args, **kwargs):
        conn = self._redis_extension.get_conn()
        source_url = self.get_argument('seed_url', '')
        url = unquote(source_url)
        priority = self.get_argument('priority', self._priority)
        batch_id = self.get_argument('batch_id', self._batch_id)
        self._meta['batch_id'] = batch_id
        meta = json.loads(self.get_argument('meta', json.dumps(self._meta)))
        meta['send_time'] = time.time()
        meta['source_url'] = source_url
        meta['batch_id'] = batch_id
        if not url:
            raise HTTPError(404, reason='invalid url!')
        if not batch_id:
            raise HTTPError(404, reason='invalid batch_id!')
        if 'send_status_redis' not in meta.keys():
            meta['send_status_redis'] = st.SEND_STATUS_REDIS
        sender = UrlSender(conn, batch_id, self._master_server)
        ret = yield sender.send_url_to_master(url=url, meta=meta, priority=priority)
        if not str(ret) == '0':
            _logger.error('Fail to send url to spider: <%s>' % source_url)
            self.write({'error': 'fail to send url to spider!'})
            self.finish()
            return
        else:
            _logger.info('success send to master <%s> [%s] [%s] ' % (url, str(meta), str(priority)))
            status = {'url': url, 'status': 'crawling'}
            try:
                self.write(status)
                self.finish()
            except Exception as e:
                _logger.error(e.message)
