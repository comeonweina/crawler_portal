# coding: utf-8
import logging
from tornado import gen
import time

_logger = logging.getLogger("StatisticIndicator")

class StatisticIndicator(object):

    def __init__(self, **kwargs):
        pass

    @gen.coroutine
    def count_from_single(self, batch_id, domain, conn, cur):
        _batch_id = batch_id
        _domain = domain
        if not _batch_id or not _domain:
            return
        _succ_num = 0
        _succ_latency = 0.0
        _fail_num = 0
        _fail_latency = 0.0
        _avg_latency = 0.0
        try:
            _num_query = "select status,count(1),sum(schedule_latency) from crawler_status_single WHERE batch_id=%s AND domain=%s group by status"
            cur.execute(_num_query, [_batch_id, _domain])
            conn.commit()
            _rows = cur.fetchall()
            for _r in _rows:
                if _r and len(_r) > 2:
                    if _r[0] == 'success':
                        _succ_num = _r[1]
                        _succ_latency = _r[2]
                    elif _r[0] == 'fail':
                        _fail_num = _r[1]
                        _fail_latency = _r[2]
            if (_fail_num + _succ_num) > 0:
                _avg_latency = (_fail_latency + _succ_latency) / (_fail_num + _succ_num)
        except Exception as e:
            _logger.exception('count from single fail %s' % e.message)
            return

        # _delete_sql = "Delete from count_from_single WHERE domain=%s AND batch_id=%s"
        _update_sql = "UPDATE count_from_single set fail_num=%s,succ_num=%s,fail_latency=%s,succ_latency=%s,avg_latency=%s WHERE domain=%s AND batch_id=%s"
        # _insert_sql = "REPLACE INTO count_from_single(batch_id,domain,fail_num,succ_num,fail_latency,succ_latency,avg_latency) values(%s,%s,%s,%s,%s,%s,%s) "
        _insert_sql = "REPLACE INTO count_from_single(batch_id,domain,fail_num,succ_num,avg_latency) values(%s,%s,%s,%s,%s) "
                      # "WHERE NOT EXISTS (SELECT 1 FROM count_from_single WHERE domain=%s AND batch_id=%s limit 1) "
        try:
            _start_time = time.time()
            ret = cur.execute(_insert_sql, [_batch_id, _domain, _fail_num, _succ_num, _avg_latency])
            conn.commit()
            _end_time = time.time()
            _logger.info('Insert statistics data into count_from_single success: [%ss] [%s] [%s]' % (str(_end_time-_start_time), _batch_id, _domain))
            # if ret > 0:
            #     pass
            # else:
            #     cur.execute(_update_sql, [_fail_num, _succ_num, _fail_latency, _succ_latency, _avg_latency, _domain, _batch_id])
            #     conn.commit()
        except Exception as e:
            _logger.exception('Insert statistics data into count_from_single fail [%s] [%s] : %s' % (_batch_id, _domain, e.message))


    @gen.coroutine
    def count_all_once(self, data, conn, cur):
        _logger.info('enter')
        _start_time = time.time()
        _batch_id = data['batch_id']
        _domain = data['domain']
        if not _batch_id or not _domain:
            return
        _query_all_sql = "select status,domain,schedule_latency from crawler_status_single WHERE batch_id=%s"
        cur.execute(_query_all_sql, [_batch_id])
        conn.commit()
        # _rows = cur.fetchall()
        _results = {}  # domain:[fail_num,succ_num,latency,domain,batch_id]
        _r = cur.fetchone()
        while _r:
            _logger.info(_r)
            if _r and len(_r) > 2:
                _status = _r[0]
                _domain = _r[1]
                _schedule_latency = _r[2]
                if _results.has_key(_domain):
                    if _status == 'success':
                        _results[_domain][1] += 1
                    elif _status == 'fail':
                        _results[_domain][0] += 1
                    _results[_domain][2] += _schedule_latency
                else:
                    if _status == 'success':
                        _results[_domain] = [0, 1, _schedule_latency, _batch_id, _domain]
                    elif _status == 'fail':
                        _results[_domain] = [1, 0, _schedule_latency, _batch_id, _domain]
            _r = cur.fetchone()

        _replace_sql = 'REPLACE into count_from_single(fail_num,succ_num,avg_latency) VALUES (%s,%s,%s) WHERE batch_id=%s AND domain=%s'
        ret = cur.executemany(_replace_sql, _results.values())
        conn.commit()
        _end_time = time.time()
        _logger.info('used_time once: %s %s' % (str(_end_time-_start_time), str(ret)))






