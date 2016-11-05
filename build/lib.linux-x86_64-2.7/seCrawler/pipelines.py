# -*- coding: utf-8 -*-

# Define your item dalines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
import  logging
import rethinkdb as r
import time
import datetime
import json
import sys
import hashlib
from urltools import *
reload(sys)
sys.setdefaultencoding('utf-8')

class SespiderPipeline(object):
    def process_item(self, item, spider):
        return item


class RethinkdbPipeline(object):
    table_name1 = "ArticleIndex"
    table_name2 = "ArticleStatus"



    def __init__(self, rdb_host, rdb_database, rdb_port, rdb_authkey):
        #Init Database connection
        self.rdb_host = rdb_host
        self.rdb_database = rdb_database
        self.rdb_port = rdb_port
        self.rdb_authkey= rdb_authkey



    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            rdb_host=crawler.settings.get('RDB_HOST', '159.203.13.58'),
            rdb_database=crawler.settings.get('RDB_DATABASE', 'FinanceCurrency'),
            rdb_port=crawler.settings.get('RDB_PORT', 28015),
            rdb_authkey=crawler.settings.get('RDB_AUTHKEY', 'atom')

        )

    def open_spider(self, spider):
        r.connect(host='159.203.13.58',
                 port=28015,
                 db='FinanceCurrency',
                 auth_key='atom').repl()

    def process_item(self, item, spider):
        dbid = hashlib.sha1(urltools.normalize(item['url'])).hexdigest()
        timestamp = str(datetime.datetime.now())
        data = {}
        data['link'] = item['url']
        data.update({'id': dbid})
        data.update({'dateinserted': timestamp})
        response = r.db(self.rdb_database).table(self.table_name1).insert(data, conflict='error').run()


        error = response.get('first_error')

        if error:
            print (error)
        else:
            data.update({'summarizable':1})
            data.update({'summarized':0})
            r.db(self.rdb_database).table(self.table_name2).insert(data).run()
        return item
