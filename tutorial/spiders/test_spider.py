# -*- coding:utf-8 -*-
import scrapy
from scrapy import Selector
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from tutorial.items import CityItem


class Bobao(scrapy.spiders.CrawlSpider):
    name = "bobao"
    allowed_domains = ["360.cn"]
    start_urls = ["http://bobao.360.cn/learning/index&page=1"]

    rules = (
        Rule(LinkExtractor(allow=("/learning/index&page=\d{1,3}")), follow=True, callback='parse_response'),
    )

    def parse_response(self, response):
        print response.url
