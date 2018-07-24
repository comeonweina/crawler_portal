# coding: utf-8
import time
import json

TORNADO_APP_SETTINGS_COOKIE_SECRET = "__TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__"

LOG_LEVEL = "DEBUG"
PORT = 8088

EXTENSIONS = [
    {
        "name": "SpiderRedisConn",
        "extension_class": "app.extensions.redis_extension.RedisExtension",
        # "redis_address": "redis://10.153.61.230:6379/2",
        "redis_address": "redis://master01.supply_spider.sjs.ted:6379/1",
    },
    {
        "name": "SchedulerRedisConn",
        "extension_class": "app.extensions.redis_extension.RedisExtension",
        # "redis_address": "redis://10.153.61.230:6379/4",
        "redis_address": "redis://:statusforfanyi@c.redis.sogou:2475/1",
    },
]

REQUEST_HANDLERS = [
    {
        "pattern": r"/status/check/",
        "handler_class": "app.request_handlers.crawler_status_handler.StatusCheckHandler",
    },
    # {
    #     # "pattern": r"/status\\?.*",
    #     # "pattern": r"/status?type=.*&random=[0-9]+",
    #     "pattern": r"/status\\?(.*)",
    #     "handler_class": "app.request_handlers.crawler_status_handler.CrawlerStatusHandler",
    #     # "key1": "value1"
    # },
    {
        "pattern": r"/crawl/",
        "handler_class": "app.request_handlers.crawl_handler.CrawlHandler",
        "master_meta": {
            "batch_id": "f_test",
            "send_time": time.time(),
            # "is_off": "Yes",
            # "is_local": "No",
            # "out_fmt": "html",
            # "ua": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36"
        },
        "master_server": "http://10.143.51.227:13164",
        # "master_server": "http://10.134.43.57:1316",
        "priority": 5


    }
]

MASTER_META = {
    "send_time": time.time(),
}
MASTER_SERVER = "http://10.143.51.227:13164"
# MASTER_SERVER = "http://10.134.43.57:1316"
PRIORITY = 5
BATCH_ID = "f_test"  # default batch_id
# BATCH_IDS = ["scheduler_test", "zhanzhang_platform", "google_meta_wap", "english_wap", "push_fech"]
BATCH_IDS = ["translate_batch"]

URL_DATA_PATH = "/search/odin/maweina/tinyfetch_http_api/data/base/urls.data"

# SCHEDULER_REDIS_ADDRESS = "redis://10.153.61.230:6379/4"
SCHEDULER_REDIS_ADDRESS = "redis://:statusforfanyi@c.redis.sogou:2475/1"
# SPIDER_REDIS_ADDRESS = "redis://10.153.61.230:6379/2"
SPIDER_REDIS_ADDRESS = "redis://master01.supply_spider.sjs.ted:6379/1"
# MYSQL_FOR_MONITOR_CONFIG = {'host':'10.153.44.170','port':3306,'user':'status','passwd':'123456','db':'statusstatistic','charset': 'utf8'}
MYSQL_FOR_MONITOR_CONFIG = {'host': '10.144.10.165', 'port': 3306, 'user': 'status', 'passwd': 'status@statistic', 'db': 'statusstatistic', 'charset': 'utf8'}

SYNCHRONIZE_DATA_PERIOD = 1*1000   # unit: millsecond
CLEAR_STATUS_CACHE_PERIOD = 24*60*60*1000   # unit: millsecond
SEND_TO_MASTER_PERIOD = 24*60*60*1000   # unit: millsecond
CLEAR_STATUS_CACHE_INTERVAL = 7*24*60*60  # unit: second

# HEADERS = {'User-Agent':'Sogou web spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm#07)'}  # default web ua
# CRAWL_TYPE = 'web' # web or wap, default web
SEND_STATUS_REDIS = True