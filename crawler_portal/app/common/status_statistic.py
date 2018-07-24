# -*- coding: utf-8 -*-
from redis import StrictRedis
import tornado
from tornado import gen, httpclient, ioloop
import logging
import json
import xmlrpclib
from requests import HTTPError
import time
import MySQLdb
from DBUtils.PersistentDB import PersistentDB
from mysqldb_utils import MySQLDBUtil

LOG_FORMAT = '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s'

logging.basicConfig(level=logging.DEBUG,
                        format=LOG_FORMAT,
                        datefmt='%Y-%m-%d %H:%M:%S')

_logger = logging.getLogger("StatusStatistic")
_logger.setLevel(logging.DEBUG)
_rh = logging.handlers.TimedRotatingFileHandler('StatusStatistic.log', when='D')
_rh.suffix = "%Y%m%d"
_fm = logging.Formatter(LOG_FORMAT)
_rh.setFormatter(_fm)
_logger.addHandler(_rh)


REDIS_ADDRESS = 'redis://:statusforspider@c.redis.sogou:2423/1'
BATCH_IDS = ["master_platform", "google_meta_wap", "english_wap", "push_fech", "google_meta"]
MYSQL_CONFIG = {'host': '10.153.44.170', 'port': 3306, 'user': 'status', 'passwd': '123456', 'db': 'statusstatistic',
           'charset': 'utf8'}

class StatusStatistic(object):
    def __init__(self):
        self._redis_conn = StrictRedis.from_url(REDIS_ADDRESS)
        self.__persist = PersistentDB(MySQLdb, 50, **MYSQL_CONFIG)

    # def count(self, period, batch_id):
    #     period = period  # B for batch_id, D for day, H for hour,5M for five minites
    #     batch_id = batch_id
    #     self._fail_hash_name = 'fetch_fail_status:%s' % batch_id
    #     self._succ_hash_name = 'fetch_success_status:%s' % batch_id
    #     if period == 'D':
    #         interval = 24*60*60
    #     elif period == 'H':
    #         interval = 60*60
    #     elif period == '5M':
    #         interval = 5*60
    #     elif period == 'B':
    #         succ_num = self._redis_conn.hlen(self._succ_hash_name)
    #         fail_num = self._redis_conn.hlen(self._fail_hash_name)
    #         return (batch_id, None, succ_num, fail_num)
    #     else:
    #         _logger.error('invalid period argument')
    #         return None
    #     succs = self._redis_conn.hgetall(self._succ_hash_name)
    #     fails = self._redis_conn.hgetall(self._fail_hash_name)
    #     cur_time = time.strftime("%Y%m%d%H0000", time.localtime())
    #     cur_hour = time.strftime("%Y-%m-%d %H:00:00", time.localtime())
    #     ts = time.mktime(time.strptime(cur_hour, "%Y-%m-%d %H:%M:%S"))
    #     succ_num = 0
    #     fail_num = 0
    #     for k, v in succs.items():
    #         # if int(json.loads(v)['status_time']) <= ts and int(json.loads(v)['status_time']) + interval > ts:
    #         if float(ts-interval) < float(json.loads(v)['status_time']) <= ts:
    #             succ_num += 1
    #     for k, v in fails.items():
    #         t = json.loads(v)['status_time']
    #         if float(ts - interval) < float(t) <= ts:  # if float(t) + interval > ts:
    #             fail_num += 1
    #     _logger.info('finish statistic: [%s] [%s]' % (batch_id, cur_time))
    #     return (fail_num, succ_num, cur_time, batch_id)


    def count_by_domain(self, batch_id):
        _query_fail_sql = "select count(num) from crawler_status_single where batch_id = '%s' and status = 'fail' group by domain"
        _query_success_sql = "select count(num) from crawler_status_single where batch_id = '%s' and status = 'success'"
        # _count_sql = "SELECT count(num),batch_id,domain,status from crawler_status_single where batch_id='%s' " \
        #              "and status_ts >= date_sub(now(),INTERVAL 2 HOUR) group by domain,status"
        _insert_sql = "INSERT into crawler_status"
        _check_sql = ""
        # try:
        #     _records = MySQLDBUtil().exec_query(_count_sql, [batch_id])
        #     if _records:
        #         _logger.info('new records: %s [%s]' % (str(len(_records)), str(batch_id)))
        #     else:
        #         _logger.warning('no new records: %s' % str(batch_id))
        # except Exception as e:
        #     _logger.exception('fail to count by domian: %s' % e.message)



    def count_5m(self, batch_id, cur_min):
        interval = 5 * 60
        batch_id = batch_id
        ts = time.mktime(time.strptime(cur_min, "%Y%m%d%H%M%S"))
        _fail_hash_name = 'fetch_fail_status:%s' % batch_id
        _succ_hash_name = 'fetch_success_status:%s' % batch_id
        if not batch_id:
            _logger.warning('No batch_id!')
            return None
        succs = self._redis_conn.hgetall(_succ_hash_name)
        fails = self._redis_conn.hgetall(_fail_hash_name)
        succ_num = 0
        fail_num = 0
        for k, v in succs.items():
            if float(ts-interval) < float(json.loads(v)['status_time']) <= ts:
                succ_num += 1
        for k, v in fails.items():
            t = json.loads(v)['status_time']
            if float(ts - interval) < float(t) <= ts:  # if float(t) + interval > ts:
                fail_num += 1
        _logger.info('finish statistic: [%s] [%s]' % (batch_id, cur_min))
        return (fail_num, succ_num, cur_min, batch_id)


    def save_result(self):
        _start = time.time()
        conn = self.__persist.connection()
        cursor = conn.cursor()
        insert_sql = "INSERT INTO crawler_status_5m(fail_num,succ_num,period,batch_id) VALUES (%s,%s,%s,%s)"
        update_sql = "update crawler_status_5m set fail_num=%s,succ_num=%s where period=%s and batch_id=%s"
        update_records = []
        insert_records = []

        cur_time = time.strftime("%Y%m%d%H%M", time.localtime())
        cur_ten = cur_time[:-1]
        cur_sigle = int(cur_time[-1])
        cur_min = ''
        if 5 <= cur_sigle < 9:
            cur_min = cur_ten + '5' + '00'
        elif cur_sigle < 5:
            cur_min = cur_ten + '0' + '00'

        for batch_id in BATCH_IDS:
            # cur_time = time.strftime("%Y%m%d%H0000", time.localtime())
            # record = self.count('H', batch_id)
            record = self.count_5m(batch_id, cur_min)
            if record:
                if self.check_exist(batch_id, cur_min):
                    update_records.append(record)
                else:
                    insert_records.append(record)
        try:
            if insert_records:
                cursor.executemany(insert_sql, insert_records)
            if update_records:
                cursor.executemany(update_sql, update_records)
            conn.commit()
            _logger.info('insert %s records !' % str(len(insert_records)))
            _logger.info('update %s records !' % str(len(update_records)))
        except Exception as e:
            _logger.exception('fail to save_result: %s' % e.message)
            conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        _used = time.time()-_start
        _logger.info('used time: %s' % str(_used))

    def check_exist(self, batch_id, period):
        if batch_id and period:
            query_sql = "SELECT * FROM crawler_status_5m WHERE batch_id=%s and period=%s"
            _conn = None
            _cur = None
            try:
                _conn = self.__persist.connection()
                _cur = _conn.cursor()
                _cur.execute(query_sql, [batch_id, period])
                _conn.commit()
                if _cur.fetchall():
                    _logger.warning('the data has existed: [%s] [%s]' % (str(batch_id), str(period)))
                    return True
                else:
                    return False
            except Exception, e:
                _logger.info(query_sql)
                _logger.exception('fail to check_exist: [%s] [%s]: %s' % (str(batch_id), str(period), e.message))
                return False
            finally:
                if _cur:
                    _cur.close()
                if _conn:
                    _conn.close()


    def run(self):
        _logger.info('begin running......')
        tornado.ioloop.PeriodicCallback(self.save_result, 5*60*1000).start()  # millsecond
        try:
            ioloop.IOLoop.current().start()
        except KeyboardInterrupt:
            _logger.warning('exit for keyboardinterrupt...')
            pass
        except Exception, e:
            _logger.exception(e.message)
        _logger.info('end running......')


if __name__ == '__main__':
    # StatusStatistic().count_5m('abc')
    StatusStatistic().run()
