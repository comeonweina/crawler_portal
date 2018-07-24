# coding: utf-8
from redis import StrictRedis
import tornado
from tornado import gen, httpclient, ioloop
import app.settings as st
import logging
import logging.handlers
import json
import xmlrpclib
from requests import HTTPError
import time
import datetime
import os
import re

LOG_FORMAT = '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'

_logger = logging.getLogger("Scheduler")
_logger.setLevel(logging.DEBUG)
_rh = logging.handlers.TimedRotatingFileHandler('./log/Scheduler.log', when='D')
_rh.suffix = "%Y%m%d"
_fm = logging.Formatter(LOG_FORMAT)
_rh.setFormatter(_fm)
_logger.addHandler(_rh)

class Scheduler(object):

    def __init__(self, **kwargs):
        self._spider_redis_conn = StrictRedis.from_url(st.SPIDER_REDIS_ADDRESS)
        self._scheduler_redis_conn = StrictRedis.from_url(st.SCHEDULER_REDIS_ADDRESS)
        self._master_server = st.MASTER_SERVER
        self._master_meta = st.MASTER_META
        self._priority = st.PRIORITY
        self._batch_id = st.BATCH_ID
        self._batch_ids = st.BATCH_IDS
        if not self._batch_id:
            _logger.error('there is no batch_id!')
            raise NameError('there is no batch_id!')
        else:
            self._fail_urls_queue_name = 'fetch_fail_urls:%s' % str(self._batch_id)
            self._success_status_hash_name = 'fetch_success_status:%s' % str(self._batch_id)
            self._fail_status_hash_name = 'fetch_fail_status:%s' % str(self._batch_id)
            self._fetch_status_queue_name = 'fetch_status:%s' % str(self._batch_id)
            # self._fetch_times_hash_name = 'fetch_times:%s' % str(self._batch_id)
        self._urls_file = st.URL_DATA_PATH
        if not self._urls_file:
            _logger.warning('no urls file to load!')
        self.ioloop = tornado.ioloop.IOLoop()


    def get_url_from_fail_queue(self):
        try:
            _fail_url = self._scheduler_redis_conn.rpop(self._fail_urls_queue_name)
            if _fail_url:
                _logger.debug('get fail_url from redis: <%s>' % _fail_url)
                return _fail_url.strip()
            else:
                _logger.debug('the fail_urls_list is empty!')
                return None
        except Exception, e:
            _logger.error('get url fail: %s' % e.message)

    def get_fail_status(self, url):
        _status_str = self._scheduler_redis_conn.hget(self._fail_status_hash_name, url)
        if _status_str:
            _status_json = json.loads(_status_str)
            _code = _status_json['code'] if _status_json else None
            _times = _status_json['fetch_times'] if _status_json else None
            _is_sent = _status_json['is_sent'] if _status_json else None
            _logger.info('fail status code: [%s], fetch_times: [%s]' % (_code, str(_times)))
            return _code, _times, _is_sent
        else:
            _logger.info('fail to get fail code for <%s>' % url)
            return None, None, None

    # def get_fetch_times_by_url(self, url):
    #     _count = self._scheduler_redis_conn.hget(self._fetch_times_hash_name, url)
    #     return _count

    @gen.coroutine
    def send_url_to_master(self, url, meta, priority=0):
        meta['batch_id'] = self._batch_id
        request_json = {
            "url": url.strip(),
            "meta": meta,
            "priority": priority
        }
        # ret = 1
        try:
            seed = json.dumps(request_json)
            server = xmlrpclib.ServerProxy(self._master_server)
            # ret = yield server.api.send.sender.send(seed)
            # raise Exception('abc')
            ret = yield gen.maybe_future(server.api.send.sender.send(seed))
        except Exception as e:
            _logger.exception('send fail: <%s> [%s]' % (url, e.message))
            self._scheduler_redis_conn.lpush(self._fail_urls_queue_name, url)
            raise gen.Return(self.handle_error(url, e, time=time.time()))
        if not str(ret) == '0':
            _logger.error('send fail: <%s> [%s]' % (url, str(ret)))
            self._scheduler_redis_conn.lpush(self._fail_urls_queue_name, url)
        else:
            _logger.info('send success: <%s> [%s]' % (url, str(ret)))


    def handle_error(self, url, e, time):
        _logger.error(e.message)
        print e.message

    # @gen.coroutine
    # def load_urls_and_send(self):
    #     if self._urls_file is None or not os.path.exists(self._urls_file):
    #         _logger.error('no urls file!')
    #         return
    #     _logger.info('[LOAD] start load urls from %s' % self._urls_file)
    #     try:
    #         with open(self._urls_file) as file:
    #             for line in file:
    #                 time.sleep(1)
    #                 yield gen.maybe_future(self.send_url_to_master(line.strip(), self._master_meta))
    #                 print line.strip()
    #     except:
    #         _logger.error('Load error!')
    #     _logger.info('[LOAD] finished!')


    # only save data in a week
    def clear_status_cache(self):
        _logger.info('begin to delete expire data...')
        for batch in self._batch_ids:
            self._batch_id = batch
            self._fail_urls_queue_name = 'fetch_fail_urls:%s' % str(self._batch_id)
            self._success_status_hash_name = 'fetch_success_status:%s' % str(self._batch_id)
            self._fail_status_hash_name = 'fetch_fail_status:%s' % str(self._batch_id)
            self._fetch_status_queue_name = 'fetch_status:%s' % str(self._batch_id)

            _fail_data = self._scheduler_redis_conn.hgetall(self._fail_status_hash_name)
            _success_data = self._scheduler_redis_conn.hgetall(self._success_status_hash_name)
            interval = 7 * 24 * 60 * 60
            cur_time = time.time()
            for k, v in _fail_data.items():
                t = json.loads(v)['status_time']
                if float(t) + interval < cur_time:
                    self._scheduler_redis_conn.hdel(self._fail_status_hash_name, k)
                    _logger.info('clear <%s>' % k)
            cur_time = time.time()
            for k, v in _success_data.items():
                t = json.loads(v)['status_time']
                if float(t) + interval < cur_time:
                    self._scheduler_redis_conn.hdel(self._success_status_hash_name, k)
                    _logger.info('clear <%s>' % k)
            _logger.info('finish delete at %s [%s]' % (str(cur_time), str(batch)))


    def synchronize_data(self):
        _logger.debug('begin synchronize data...')
        for batch in self._batch_ids:
            self._batch_id = batch
            self._fail_urls_queue_name = 'fetch_fail_urls:%s' % str(self._batch_id)
            self._success_status_hash_name = 'fetch_success_status:%s' % str(self._batch_id)
            self._fail_status_hash_name = 'fetch_fail_status:%s' % str(self._batch_id)
            self._fetch_status_queue_name = 'fetch_status:%s' % str(self._batch_id)

            while True:
                _status_str = self._spider_redis_conn.rpop(self._fetch_status_queue_name)
                if not _status_str:
                    _logger.debug('status list is empty!')
                    break
                _status_json = json.loads(_status_str)
                if _status_json:
                    _url = _status_json['url']
                    _stat = _status_json['status']
                    _status_json['group'] = _stat['group']
                    _status_json['code'] = _stat['code']
                    if _stat['group'] == 'HTTP' and re.search(r'(2|3)\d{2}', _stat['code']):
                        _status_json['status'] = 'success'
                        self._scheduler_redis_conn.hdel(self._fail_status_hash_name, _url)
                        self._scheduler_redis_conn.hset(self._success_status_hash_name, _url, json.dumps(_status_json))
                    else:
                        _status_json['status'] = 'fail'
                        _status_json['is_sent'] = False
                        if self._scheduler_redis_conn.hget(self._success_status_hash_name, _url):
                            continue
                        cur_status = self._scheduler_redis_conn.hget(self._fail_status_hash_name, _url)
                        if not cur_status:
                            _status_json['fetch_times'] = 1
                        else:
                            _times = json.loads(cur_status)['fetch_times']
                            _status_json['fetch_times'] = int(_times) + 1
                        self._scheduler_redis_conn.lpush(self._fail_urls_queue_name, _url)
                        self._scheduler_redis_conn.hset(self._fail_status_hash_name, _url, json.dumps(_status_json))
                    _logger.info('synchronize data <%s>' % _url)
            _logger.info('finish synchronize data [%s]' % str(batch))

    def send_loop(self):
        print 'begin sending fail url...'
        # index = 0
        for batch in self._batch_ids:
            self._batch_id = batch
            self._fail_urls_queue_name = 'fetch_fail_urls:%s' % str(self._batch_id)
            self._success_status_hash_name = 'fetch_success_status:%s' % str(self._batch_id)
            self._fail_status_hash_name = 'fetch_fail_status:%s' % str(self._batch_id)
            self._fetch_status_queue_name = 'fetch_status:%s' % str(self._batch_id)

            start_time = datetime.datetime.now()
            while True:
                # time.sleep(1)
                gen.sleep(1)
                # index += 1
                try:
                    url = self.get_url_from_fail_queue()
                    if not url:
                        _logger.info('no url to send [%s]' % str(batch))
                        break
                    if self._scheduler_redis_conn.hget(self._success_status_hash_name, url):
                        continue
                    _status_str = self._scheduler_redis_conn.hget(self._fail_status_hash_name, url)
                    if _status_str:
                        _status_json = json.loads(_status_str)
                        code, times, is_sent = _status_json['code'], _status_json['fetch_times'], _status_json[
                            'is_sent']
                        if not code:
                            continue
                        elif re.search(r'4[0-9]{2}', code):
                            _logger.info('discard for code [%s]: <%s>' % (code, url))
                            continue
                        if times and int(times) >= 3:
                            _logger.info('discard for fetch times exceed: <%s>' % url)
                            continue
                        if is_sent:
                            _logger.warning('url has sent <%s>' % url)
                            continue
                        self.send_url_to_master(url=url, meta=self._master_meta, priority=self._priority)
                        _status_json['is_sent'] = True
                        _status_json['fetch_times'] = (int(times) + 1) if times else 1
                        self._scheduler_redis_conn.hset(self._fail_status_hash_name, url, json.dumps(_status_json))

                except KeyboardInterrupt:
                    break
                except Exception as e:
                    _logger.exception(e)
                    break
            end_time = datetime.datetime.now()
            used_seconds_time = (end_time - start_time).total_seconds()
            _logger.debug('send fail urls used_time: %s seconds [%s]' % (used_seconds_time, str(batch)))

    def run(self):
        # self.load_urls_and_send()
        _logger.info('begin running....')
        tornado.ioloop.PeriodicCallback(self.synchronize_data, 60*60*1000).start()  # millsecond
        tornado.ioloop.PeriodicCallback(self.send_loop, 24*60*60*1000).start()  # millsecond
        tornado.ioloop.PeriodicCallback(self.clear_status_cache, 24*60*60*1000).start()  # millsecond

        try:
            ioloop.IOLoop.current().start()
        except KeyboardInterrupt:
            _logger.warning('scheduler exit for keyboardinterrupt...')
            pass
        except Exception, e:
            _logger.exception(e.message)

    # def run1(self):
    #     _logger.info('begin to send....')
    #     start_time = datetime.datetime.now()
    #     while True:
    #         try:
    #             url = self.get_url_from_fail_queue()
    #             if not url:
    #                 break
    #             self.send_url_to_master(url=url, meta=self._master_meta)
    #         except KeyboardInterrupt:
    #             break
    #         except Exception as e:
    #             _logger.exception(e)
    #             break
    #     end_time = datetime.datetime.now()
    #     used_seconds_time = (end_time - start_time).total_seconds()
    #     _logger.debug('used_time: %s seconds' % used_seconds_time)
    #     _logger.info('sending exiting...')

    def count(self):
        succ_num = self._scheduler_redis_conn.hlen(self._success_status_hash_name)
        fail_num = self._scheduler_redis_conn.hlen(self._fail_status_hash_name)
        return succ_num, fail_num



if __name__ == "__main__":
    sc = Scheduler()
    succ_num, fail_num = sc.count()
    print succ_num, fail_num
    # sc.synchronize_data()
    # sc.clear_status_cache()
    sc.run()
