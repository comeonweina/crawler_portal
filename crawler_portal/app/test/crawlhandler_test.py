# -*- coding: utf-8 -*-

import requests
import time
from urllib import quote
import json

def post_request_to_crawlHandler():
    # url = 'http://10.134.124.230:8088/crawl/'  # zhanzhang
    # url = 'http://10.141.180.224:8088/status/check/'
    url = 'http://10.153.51.227:8088/crawl/' # local
    url_file = open('/search/odin/maweina/tinyfetch_http_api/data/base/urls.data', 'r')
    line = str(url_file.readline())
    index = 0
    _start = time.time()

    while line and index < 809:
        seed_url = line.strip()
        # seed_url = 'http://www.jmw.ac.cn/sunday/site/product/findOne?id=1011#jianjie'
        # seed_url = 'http://www.whoamivr.com/h-nd-814.html#_np=2_307'
        # seed_url = 'http://www.hnbdfyy.com.cn/bdfzl/5215.html?'
        # seed_url = 'http://cszy.01ny.cn/zl/2772.html?'
        # seed_url = 'http://www.chinawuda.com/about.shtml?url=app/tms#tms_fun'
        # seed_url = 'https://kafka-python.readthedocs.io/en/master/usage.html'
        # seed_url = 'https://m.baidu.com/?from=1012852s'
        # seed_url = 'www.youyouyouka.com/nd.jsp?id=27#_np=105_374'
        # seed_url = 'http://yekasa.com/2018/04/18/%E5%AD%A6%E7%94%9F%E5%9C%A8%E6%A0%A1%E6%BB%91%E5%80%92%E5%8F%97%E4%BC%A4-%E4%BF%9D%E9%99%A9%E5%85%AC%E5%8F%B8%E3%80%81%E5%AD%A6%E6%A0%A1%E5%8F%8C%E8%B5%94%E5%81%BF/'
        # seed_url = 'https://www.baidu.com/'
        # seed_url = 'http://www.lzbdyy.net/yc/218.html'
        seed_url = 'https://doc.scrapy.org/en/latest/topics/settings.html?highlight=BOT_NAME'

        # url = 'http://translate.schedule.nm.ted:8088/status/check/'
        # url = 'http://10.153.51.227:8088/status/check/'  # local

        url="http://10.153.51.227:8088/crawl/"
        # url="http://wap.schedule.djt.ted:8088/crawl/"
        # url="http://supply.schedule.sogou/crawl/"
        # url="http://10.134.124.230:8088/crawl/"
        meta = {"send_time": time.time(),
                "wap_kafka_store": True,
                "User-Agent": "Mozilla/5.0 (Linux; Android 6.0.1) AppleWebKit/601.1 (KHTML,like Gecko) Version/9.0 Mobile/13B143 Safari/601.1Mozilla/5.0 (Linux; Android 6.0.1) AppleWebKit/601.1 (KHTML,like Gecko) Version/9.0 Mobile/13B143 Safari/601.1 (compatible; Sogou wap spider/4.0; +http://www.sogou.com/docs/help/webmasters.htm#07)",
                "depth": 1
                }
        data = {'seed_url': seed_url, 'batch_id': 'wap_supply_crawl', 'priority': 16, 'meta': json.dumps(meta)}

        # data = {'seed_url': seed_url, 'batch_id': 'zhanzhang_platform', 'priority': 10}

        rsp = requests.post(url=url, data=data)

        # url = 'http://wap.schedule.djt.ted:8088/status/check/'
        # data = {'url': seed_url, 'batch_id': 'wap_supply_crawl'}
        # rsp = requests.post(url=url, data=data)

        # if index >= 800:
        #     # print index
        #     print time.time()
        #     rsp = requests.post(url=url, data=data)
        #     print seed_url
        #     # print quote(seed_url)
        # index += 1
        # line = url_file.readline()
    url_file.close()
    print (time.time()-_start)


if __name__ == '__main__':

    seed_url = 'https://doc.scrapy.org/en/latest/topics/settings.html?highlight=BOT_NAME'
    # seed_url = 'https://redisdesktop.com/download'
    url = "http://10.153.51.227:8088/crawl/"
    meta = {"send_time": time.time(),
            # "wap_kafka_store": True,
            # "User-Agent": "Mozilla/5.0 (Linux; Android 6.0.1) AppleWebKit/601.1 (KHTML,like Gecko) Version/9.0 Mobile/13B143 Safari/601.1Mozilla/5.0 (Linux; Android 6.0.1) AppleWebKit/601.1 (KHTML,like Gecko) Version/9.0 Mobile/13B143 Safari/601.1 (compatible; Sogou wap spider/4.0; +http://www.sogou.com/docs/help/webmasters.htm#07)",
            "depth": 1
            }
    data = {'seed_url': seed_url, 'batch_id': 'scheduler_test', 'priority': 16, 'meta': json.dumps(meta)}

    # data = {'seed_url': seed_url, 'batch_id': 'zhanzhang_platform', 'priority': 10}

    rsp = requests.post(url=url, data=data)
    pass
    # post_request_to_crawlHandler()
