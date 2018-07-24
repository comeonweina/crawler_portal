# -*- coding: utf-8 -*-
import MySQLdb
from DBUtils.PersistentDB import PersistentDB

MYSQL_CONFIG = {'host': '10.153.44.170', 'port': 3306, 'user': 'status', 'passwd': '123456', 'db': 'statusstatistic',
           'charset': 'utf8'}

class MySQLDBUtil(object):
    __persist = None
    __config = {}

    def __init__(self):
        self._config = MYSQL_CONFIG
        self.__persist = PersistentDB(MySQLdb, 50, **self._config)

    def connection(self):
        return self.__persist.connection()

    # 统一执行SQL
    def exec_sql(self, sql=None, args=None):
        if sql:
            _cn = None
            _cur = None
            try:
                _cn = self.connection()
                _cur = _cn.cursor()
                _cur.execute(sql, args)
                _cn.commit()
                return _cur.rowcount
            finally:
                if _cur:
                    _cur.close()
                if _cn:
                    _cn.close()

    # 统一执行SQL
    def exec_query(self, sql=None, args=None):
        if sql:
            _cn = None
            _cur = None
            try:
                _cn = self.connection()
                _cur = _cn.cursor()
                _cur.execute(sql, args)
                _cn.commit()
                return _cur.fetchall()
            finally:
                if _cur:
                    _cur.close()
                if _cn:
                    _cn.close()

    # 统一执行SQL
    def exec_insert(self, sql=None, args=None):
        if sql:
            _cn = None
            _cur = None
            try:
                _cn = self.connection()
                _cur = _cn.cursor()
                _cur.executemany(sql, args)
                _cn.commit()
                return _cur.rowcount
            finally:
                if _cur:
                    _cur.close()
                if _cn:
                    _cn.close()