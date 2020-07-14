# -*- coding: utf-8 -*-

import scrapy


class WikiItem(scrapy.Item):
  cat = scrapy.Field()
  title = scrapy.Field()
  url = scrapy.Field()
  text = scrapy.Field()
  pass
