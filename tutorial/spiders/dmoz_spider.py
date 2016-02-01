# -*- coding:utf-8 -*-
import scrapy
from scrapy import Selector
from tutorial.items import CityItem
import datetime


class DmozSpider(scrapy.Spider):
    name = "aqi"
    allowed_domains = ["mep.gov.cn"]
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    month_before_yesterday = (datetime.datetime.now() - datetime.timedelta(days=31)).strftime("%Y-%m-%d")
    start_urls = [
        "http://datacenter.mep.gov.cn/report/air_daily/air_dairy.jsp?city=&startdate=" + month_before_yesterday + "&enddate=" + yesterday + "&page=1",
    ]

    def parse(self, response):
        sel = Selector(response)
        cities = sel.css("#report1 tr")

        for city in cities:
            item = CityItem()
            item["id"] = city.css(":nth-child(1)::text").extract()
            item["name"] = city.css(":nth-child(2)::text").extract()
            item["date"] = city.css(":nth-child(3)::text").extract()
            item["AQI"] = city.css(":nth-child(4)::text").extract()
            item["level"] = city.css(":nth-child(5)::text").extract()
            item["prime"] = city.css(":nth-child(6)::text").extract()
            yield item

        next_page = int(sel.css("#report1 tr:nth-last-child(2) input:first-child::attr(value)")[0].extract()) + 1
        total_page = int(sel.css("#report1 tr:nth-last-child(2) td:first-child font::text")[1].extract())
        if next_page <= total_page:
            yield scrapy.Request(
                url="http://datacenter.mep.gov.cn/report/air_daily/air_dairy.jsp?city=&startdate=" + self.month_before_yesterday + "&enddate=" + self.yesterday + "&page=" + str(
                    next_page), callback=self.parse)
