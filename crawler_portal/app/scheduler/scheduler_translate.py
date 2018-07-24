# coding: utf-8
from redis import StrictRedis
import tornado
from tornado import gen, httpclient, ioloop
import app.settings as st
import logging
import json
import xmlrpclib
from requests import HTTPError
import time
import datetime
import os
import re
import MySQLdb
from DBUtils.PersistentDB import PersistentDB
from urllib import quote
from influxdb import InfluxDBClient, exceptions
from urlparse import *
from statistic_indicator import StatisticIndicator



# import os
# import sys
# sys.path.append('..')
# parentdir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.insert(0,parentdir)
# import settings as st


LOG_FORMAT = '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'
logging.basicConfig(level=logging.DEBUG,
                        format=LOG_FORMAT,
                        datefmt='%Y-%m-%d %H:%M:%S')
_logger = logging.getLogger("Scheduler")
_logger.setLevel(logging.DEBUG)
_rh = logging.handlers.TimedRotatingFileHandler('./log/Scheduler_translate.log', when='D')
_rh.suffix = "%Y%m%d"
_fm = logging.Formatter(LOG_FORMAT)
_rh.setFormatter(_fm)
_logger.addHandler(_rh)

class Scheduler(object):

    def __init__(self, **kwargs):

        self.SYNCHRONIZE_DATA_PERIOD = st.SYNCHRONIZE_DATA_PERIOD
        self.CLEAR_STATUS_CACHE_PERIOD = st.CLEAR_STATUS_CACHE_PERIOD
        self.SEND_TO_MASTER_PERIOD = st.SEND_TO_MASTER_PERIOD
        self.CLEAR_STATUS_CACHE_INTERVAL = st.CLEAR_STATUS_CACHE_INTERVAL

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
        self._mysql_config = st.MYSQL_FOR_MONITOR_CONFIG
        self.__persist = PersistentDB(MySQLdb, 50, **self._mysql_config)
        self._influxdb_client = InfluxDBClient('10.144.10.165', 8086, 'status', 'status@monitordb', 'status')

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


    # not used current
    @gen.coroutine
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
            interval = 7 * 24 * 60 * 60 if self.CLEAR_STATUS_CACHE_INTERVAL is None else self.CLEAR_STATUS_CACHE_INTERVAL
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

    @gen.coroutine
    def clear_expire_data(self):
        _logger.info('begin to delete expire data...')
        _start = time.time()
        for batch in self._batch_ids:
            self._batch_id = batch
            self._fail_urls_queue_name = 'fetch_fail_urls:%s' % str(self._batch_id)
            self._success_status_hash_name = 'fetch_success_status:%s' % str(self._batch_id)
            self._fail_status_hash_name = 'fetch_fail_status:%s' % str(self._batch_id)
            self._fetch_status_queue_name = 'fetch_status:%s' % str(self._batch_id)

            interval = 7 * 24 * 60 * 60 if self.CLEAR_STATUS_CACHE_INTERVAL is None else self.CLEAR_STATUS_CACHE_INTERVAL
            cur_time = time.time()

            _first = True
            _cursor = 0
            while _cursor != 0 or _first:
                _first = False
                _fail_data = self._scheduler_redis_conn.hscan(self._fail_status_hash_name, _cursor)
                _cursor = _fail_data[0]
                _records = _fail_data[1]
                if _records:
                    yield self.judge_and_delete(self._fail_status_hash_name, _records, interval, cur_time)

            _first = True
            _cursor = 0
            while _cursor != 0 or _first:
                _first = False
                _success_data = self._scheduler_redis_conn.hscan(self._success_status_hash_name, _cursor)
                _cursor = _success_data[0]
                _records = _success_data[1]
                if _records:
                    yield self.judge_and_delete(self._success_status_hash_name, _records, interval, cur_time)
            _logger.info('finish delete at %s [%s]' % (str(cur_time), str(batch)))
        _logger.info('delete expire data used time: %s' % str(time.time()-_start))


    @gen.coroutine
    def judge_and_delete(self, hash_name, data_dict, interval, cur_time):
        for k, v in data_dict.items():
            t = json.loads(v)['status_time']
            if float(t) + interval < cur_time:
                # _logger.info(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t)))
                self._scheduler_redis_conn.hdel(hash_name, k)
                _logger.info('clear <%s> [%s]' % (k, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(t))))


    @gen.coroutine
    def synchronize_data(self):
        _logger.debug('begin synchronize data...')
        _mysql_conn = None
        _mysql_cur = None
        try:
            _mysql_conn = self.__persist.connection()
            _mysql_cur = _mysql_conn.cursor()

            for batch in self._batch_ids:
                self._batch_id = batch
                self._fail_urls_queue_name = 'fetch_fail_urls:%s' % str(self._batch_id)
                self._success_status_hash_name = 'fetch_success_status:%s' % str(self._batch_id)
                self._fail_status_hash_name = 'fetch_fail_status:%s' % str(self._batch_id)
                self._fetch_status_queue_name = 'fetch_status:%s' % str(self._batch_id)
                _start = time.time()
                count = 0
                while True:
                    _status_str = self._spider_redis_conn.rpop(self._fetch_status_queue_name)
                    if not _status_str:
                        _logger.debug('status list is empty! [%s]' % str(batch))
                        break
                    _status_json = json.loads(_status_str)
                    _parse_dict = {}
                    if _status_json:
                        # _url = _status_json['url']
                        _stat = _status_json['status']
                        _meta = _status_json['meta']
                        if not _meta or not _meta.get('send_time', None) or not _meta.get('source_url', None):
                            _logger.info('discard for meta lack param send_time or source_url [%s] %s' % (batch,_meta))
                            continue
                        _url = _meta.get('source_url', None)
                        _parse_dict['url'] = _url
                        _parse_dict['group'] = _stat.get('group', None)
                        _parse_dict['code'] = _stat.get('code', None)
                        _parse_dict['send_time'] = _meta.get('send_time', None)
                        if _parse_dict is None:
                            _logger.info('send_time is None')
                        _parse_dict['domain'] = _meta.get('host_key', None) if _meta.get('host_key', None) else urlparse(_url).netloc
                        _parse_dict['status_time'] = _status_json.get('status_time', None)
                        _parse_dict['download_latency'] = _status_json.get('download_latency', None)
                        _parse_dict['fetch_time'] = _meta.get('fetch_time', None)
                        if _parse_dict['send_time'] and _parse_dict['status_time']:
                            _parse_dict['schedule_latency'] = _parse_dict['status_time'] - _parse_dict['send_time']
                        else:
                            _parse_dict['schedule_latency'] = None

                        if _stat['group'] == 'HTTP' and re.search(r'(2|3)\d{2}', _stat['code']):
                            _parse_dict['status'] = 'success'
                            yield self.save_redisdb(_url, _parse_dict, 'success', batch)

                        else:
                            _parse_dict['status'] = 'fail'
                            _parse_dict['is_sent'] = False
                            if self._scheduler_redis_conn.hget(self._success_status_hash_name, _url):
                                continue
                            cur_status = self._scheduler_redis_conn.hget(self._fail_status_hash_name, _url)
                            if not cur_status:
                                _parse_dict['fetch_times'] = 1
                            else:
                                _times = json.loads(cur_status)['fetch_times']
                                _parse_dict['fetch_times'] = int(_times) + 1
                            yield self.save_redisdb(_url, _parse_dict, 'fail', batch)

                        _mysql_json = _parse_dict
                        _mysql_json['batch_id'] = batch
                        yield self.save_mysqldb(_mysql_json, _mysql_conn, _mysql_cur)
                        # yield self.save_influxdb(_mysql_json)
                    count += 1
                _used = time.time()-_start
                _logger.info('used_time:' + str(_used))
                _logger.info('count:' + str(count))
                _logger.info('speed:' + str(count/_used) + ' every second')
                _logger.info('finish synchronize data [%s]' % str(batch))
        except Exception as e:
            _logger.exception('fail to synchronize data: %s' % e.message)
        finally:
            if _mysql_cur:
                _mysql_cur.close()
            if _mysql_conn:
                _mysql_conn.close()
            _logger.info('close mysql connection!')

    @gen.coroutine
    def save_redisdb(self, url, status, type, batch_id):
        self._scheduler_redis_conn.sadd('fetch_domains:' + str(batch_id), status['domain'])
        if type == 'fail':
            self._scheduler_redis_conn.lpush(self._fail_urls_queue_name, url)
            self._scheduler_redis_conn.hset(self._fail_status_hash_name, url, json.dumps(status))
        elif type == 'success':
            self._scheduler_redis_conn.hdel(self._fail_status_hash_name, url)
            self._scheduler_redis_conn.hset(self._success_status_hash_name, url, json.dumps(status))
        _logger.info("save data to redisdb <%s>" % url)


    @gen.coroutine
    def save_mysqldb(self, mysql_json, conn, cur):
        _mysql_json = mysql_json
        _g = _mysql_json.pop('group')
        _mysql_json['group_type'] = _g
        _mysql_json['status_ts'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(_mysql_json['status_time']))
        _insert_sql = "INSERT INTO crawler_status_single(%s) VALUES (%s)" % (','.join(_mysql_json.keys()), ','.join(['%s'] * len(_mysql_json)))
        try:
            cur.execute(_insert_sql, _mysql_json.values())
            conn.commit()
            _logger.info('synchronize data to mysql <%s>' % _mysql_json['url'])
        except Exception as e:
            _logger.exception('fail to synchronize data to mysql <%s> : %s' % (mysql_json['url'], e.message))


        # count_by_domain
        # _query_sql = "SELECT succ_num,fail_num,latency FROM count_by_domain WHERE batch_id=%s AND domain=%s"
        # _status = mysql_json['status']
        # if _mysql_json.get('schedule_latency', None):
        #     _row_latency = float('%.6f' % _mysql_json['schedule_latency'])
        # else:
        #     _row_latency = 0.0
        # try:
        #     cur.execute(_query_sql, [_mysql_json['batch_id'], _mysql_json['domain']])
        #     conn.commit()
        #     _row = cur.fetchone()
        #     if _row:
        #         _latency = _row_latency + (_row[2] if _row[2] else 0.0)
        #         if _status == 'success':
        #             _succes_num = _row[0] + 1
        #             cur.execute("UPDATE count_by_domain SET succ_num=%s,latency=%s WHERE batch_id=%s AND domain=%s", [_succes_num, _latency, _mysql_json['batch_id'], _mysql_json['domain']])
        #         elif _status == 'fail':
        #             _fail_num = _row[1] + 1
        #             cur.execute("UPDATE count_by_domain SET fail_num=%s,latency=%s WHERE batch_id=%s AND domain=%s", [_fail_num,_latency, _mysql_json['batch_id'], _mysql_json['domain']])
        #         conn.commit()
        #     else:
        #         _latency = _row_latency
        #         if _status == 'success':
        #             cur.execute("INSERT INTO count_by_domain(succ_num,batch_id,domain,latency) values(%s,%s,%s,%s)", [1, _mysql_json['batch_id'], _mysql_json['domain'], _latency])
        #         elif _status == 'fail':
        #             cur.execute("INSERT INTO count_by_domain(fail_num,batch_id,domain,latency) values(%s,%s,%s,%s)", [1, _mysql_json['batch_id'], _mysql_json['domain'], _latency])
        #         conn.commit()
        #     _logger.info('count by domain and save to mysql <%s>' % _mysql_json['url'])
        # except Exception as e:
        #     _logger.exception('fail to count by domain data to mysql <%s> : %s' % (mysql_json['url'], e.message))

    @gen.coroutine
    def count_by_domain(self):
        _logger.debug('begin count by domain...')
        _mysql_conn = None
        _mysql_cur = None
        st = StatisticIndicator()
        try:
            _mysql_conn = self.__persist.connection()
            _mysql_cur = _mysql_conn.cursor()
            _start = time.time()
            for batch in self._batch_ids:
                _set_name = 'fetch_domains:%s' % str(batch)
                _domain = self._scheduler_redis_conn.spop(_set_name)
                while _domain:
                    yield st.count_from_single(batch, _domain, _mysql_conn, _mysql_cur)
                    _domain = self._scheduler_redis_conn.spop(_set_name)
            _end_time = time.time()
            _logger.info('finish count by domain used: %ss' % str(_end_time-_start))
        except Exception as e:
            _logger.exception('count indicators by domain fail: %s' % e.message)
        finally:
            if _mysql_cur:
                _mysql_cur.close()
            if _mysql_conn:
                _mysql_conn.close()
            _logger.info('close mysql connection for count indicators!')



    @gen.coroutine
    def save_influxdb(self, data):
        data_dict = data
        try:
            body = [
                {
                    "measurement": "crawlerstatus",
                    "tags": {
                        "batch_id": data_dict['batch_id'],
                        "status": data_dict['status'],
                        "site": urlparse(data_dict['url']).netloc,
                        "group_type":data_dict['group'] if 'group' in data_dict.keys() else None
                    },
                    "fields": {
                        "url": data_dict['url'],
                        "download_latency": float(data_dict['download_latency']) if data_dict.get('download_latency', None) else None,
                        "num":1
                    },
                    "time": int(data_dict['status_time']*10**9),
                }
            ]
            self._influxdb_client.write_points(body)
            _logger.info('synchronize data to influxdb <%s>' % data_dict['url'])
        except exceptions.InfluxDBClientError as e:
            _logger.exception('fail to synchronize data to influxdb <%s> : %s' % (data_dict['url'], e.message))

    @gen.coroutine
    def send_loop(self):
        _logger.info('begin sending fail url...')
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
                # gen.sleep(1)
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
                        code, times, is_sent = _status_json['code'], _status_json['fetch_times'], _status_json['is_sent']
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
        _logger.info('begin running....')
        # tornado.ioloop.PeriodicCallback(self.synchronize_data, self.SYNCHRONIZE_DATA_PERIOD).start()  # millsecond
        tornado.ioloop.PeriodicCallback(self.count_by_domain, 6*1000).start()  # millsecond
        # tornado.ioloop.PeriodicCallback(self.send_loop, self.SEND_TO_MASTER_PERIOD).start()  # millsecond
        # tornado.ioloop.PeriodicCallback(self.clear_expire_data, self.CLEAR_STATUS_CACHE_PERIOD).start()  # millsecond

        try:
            ioloop.IOLoop.current().start()
        except KeyboardInterrupt:
            _logger.warning('scheduler exit for keyboardinterrupt...')
            pass
        except Exception, e:
            _logger.exception(e.message)

    def count(self):
        succ_num = self._scheduler_redis_conn.hlen(self._success_status_hash_name)
        fail_num = self._scheduler_redis_conn.hlen(self._fail_status_hash_name)
        return succ_num, fail_num


if __name__ == "__main__":
    sc = Scheduler()
    sc.run()
