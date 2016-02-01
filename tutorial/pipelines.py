# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import codecs
from twisted.enterprise import adbapi
from scrapy.exceptions import DropItem
import MySQLdb
import MySQLdb.cursors
import logging


class JsonWriterPipeline(object):
    def __init__(self):
        self.file = codecs.open("cities.json", "wb", encoding="utf-8")

    def process_item(self, item, spider):
        if len(item['id']) > 0 and item['id'][0].isdigit():
            line = json.dumps(dict(item), ensure_ascii=False) + "\n"
            self.file.write(line)
            return item
        else:
            raise DropItem('not a city!')

    def spider_closed(self, spider):
        self.file.close()


class MysqlPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbargs = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DBNAME"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
            cursorclass=MySQLdb.cursors.DictCursor,
            charset="utf8",
            use_unicode=True
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
        return cls(dbpool)

    def process_item(self, item, spider):
        d = self.dbpool.runInteraction(self._do_insert, item, spider)
        d.addErrback(self._handle_error, item, spider)
        d.addBoth(lambda _: item)
        return d

    def _handle_error(self, failure, item, spider):
        logging.log(failure)

    def open_spider(self, spider):
        """
        things to do before open spider
        :param spider:
        :return:
        """
        d = self.dbpool.runInteraction(self._drop_table)
        d = self.dbpool.runInteraction(self._create_table)

    def _drop_table(self, conn):
        conn.execute("""
        DROP table if EXISTS `city_daily_aqi`
                     """)

    def _create_table(self, conn):
        conn.execute("""
        CREATE TABLE `city_daily_aqi` (
        `id` int(11) NOT NULL,
        `name` varchar(45) DEFAULT NULL,
        `date` datetime DEFAULT NULL,
        `AQI` varchar(45) DEFAULT NULL,
        `level` varchar(45) DEFAULT '污染等级',
        `prime` varchar(45) DEFAULT '首要污染物',
        PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """)

    def _do_insert(self, conn, item, spider):
        id = item["id"][0]
        if id.isdigit():
            name = item["name"][0] if len(item["name"]) > 0 else ""
            date = item["date"][0] if len(item["date"]) > 0 else ""
            AQI = item["AQI"][0] if len(item["AQI"]) > 0 else ""
            level = item["level"][0] if len(item["level"]) > 0 else ""
            prime = item["prime"][0] if len(item["prime"]) > 0 else ""
            conn.execute("""
            insert into city_daily_aqi (id, name, date, AQI, level, prime)
            VALUES
            (%s, %s, %s, %s, %s, %s)
            """, (id, name, date, AQI, level, prime))
