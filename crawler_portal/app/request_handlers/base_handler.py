import json
from tornado.web import RequestHandler,HTTPError
import logging
import app.settings as st
from os_tornado.decorators.json_response import jsonify
import time

_logger = logging.getLogger('BaseHandler')

@jsonify
class BaseHandler(RequestHandler):
    def initialize(self, **kwargs):
        self._redis_extension = self.application.manager.get_extension('SchedulerRedisConn')
        self._batch_id = st.BATCH_ID
        if not self._batch_id:
            _logger.error('batch_id is not configured!')
            raise NameError('no batch_id!')
        self._fail_hash_name = 'fetch_fail_status:%s' % self._batch_id
        self._succ_hash_name = 'fetch_success_status:%s' % self._batch_id
        self._master_server = st.MASTER_SERVER

    def get(self, *args, **kwargs):
        raise NotImplementedError('Not implemented.')

    # def write_error(self, status_code, **kwargs):
    #     pass


