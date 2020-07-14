from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy import Request

import re
import unicodedata
from scrapy.crawler import CrawlerProcess
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from wiki.items import WikiItem

from hdbcli import dbapi
import numpy as np


# guter post
# https://stackoverflow.com/questions/8903730/scrapy-start-urls

def hana_init(ip, port, user, pw):
  connection = dbapi.connect(ip, port, user, pw)
  cursor = connection.cursor()
  return cursor


# marius
# hana_ip = '34.255.196.145'
# max
hana_ip = '52.17.42.3'
cursor = hana_init(hana_ip, 39015, 'SYSTEM', 'Glorp2018!')


class WikiSpider(CrawlSpider):
  """
  the Page Spider for wikipedia
  """

  name = "wiki"
  allowed_domains = ["en.wikipedia.org"]

  start_urls = [
      # "https://en.wikipedia.org/wiki/Category:Cats",
      # "https://en.wikipedia.org/wiki/Category:Dogs",
      # "https://en.wikipedia.org/wiki/Category:Fish",
      # "https://en.wikipedia.org/wiki/Category:Countries",
      "https://en.wikipedia.org/wiki/Category:Video_games"
  ]
  base_url = 'https://en.wikipedia.org'
  category_url = 'https://en.wikipedia.org/wiki/Category:'
  article_url = 'https://en.wikipedia.org/wiki/'

  rules = (
      Rule(LinkExtractor(
          allow="https://en\.wikipedia\.org/wiki/.+",
          deny=[
              "https://en\.wikipedia\.org/wiki/Wikipedia.*",
              "https://en\.wikipedia\.org/wiki/Main_Page",
              "https://en\.wikipedia\.org/wiki/Free_Content",
              "https://en\.wikipedia\.org/wiki/Talk.*",
              "https://en\.wikipedia\.org/wiki/Portal.*",
              "https://en\.wikipedia\.org/wiki/Special.*",
              "https://en\.wikipedia\.org/wiki/Template.*",
              "https://en\.wikipedia\.org/wiki/User.*",
              "https://en\.wikipedia\.org/wiki/Help.*",
              "https://en\.wikipedia\.org/wiki/File:.*",
              "https://en\.wikipedia\.org/wiki/Wikipedia:.*"
          ],
          restrict_xpaths=[
              "//div[@class='mw-category-generated']"
          ]),
          callback='parse_wiki'),
  )

  category = start_urls[0].split(':')[2]

  def parse_wiki(self, response):
    def hana_upload(cursor, data):
      cat = data[0].replace("'", "''")
      title = data[1].replace("'", "''")
      url = data[2].replace("'", "''")
      text = data[3].replace("'", "''")
      sql = f"insert into \"SYSTEM\".\"WIKI\" (CAT, TITLE, URL, TEXT) VALUES ('{cat}','{title}','{url}','{text}')"
      cursor.execute(sql)

    def text_cleaner(value):
      value = ' '.join(value)
      value = value.replace('\n', '')
      value = unicodedata.normalize("NFKD", value)
      value = re.sub(r' , ', ', ', value)
      value = re.sub(r' \( ', ' (', value)
      value = re.sub(r' \) ', ') ', value)
      value = re.sub(r' \)', ') ', value)
      value = re.sub(r'\[\d.*\]', ' ', value)
      value = re.sub(r' +', ' ', value)
      return value.strip()

    print(f'Found a page: {response.url}')

    base_url = self.base_url
    category_url = self.category_url
    article_url = self.article_url

    # if category then crawl more pages
    if response.url.startswith(category_url):
      print(f'Crawl category: {response.url}')
      # all links
      # links = response.xpath("//a/@href").extract()
      # category links
      links = response.xpath("//div[@class='mw-category-generated']//a/@href").extract()
      regex = re.compile(r'^/wiki/.*')
      selected_links = list(filter(regex.search, links))
      for link in selected_links:
        absolute_next_page_url = base_url + link
        # print(absolute_next_page_url)
        yield Request(absolute_next_page_url)
    # elif articale then fetch page
    elif response.url.startswith(article_url):
      print(f'Crawl article: {response.url}')
      item = WikiItem()
      body = BeautifulSoup(response.body)

      item['cat'] = self.category
      item['url'] = response.url
      item['title'] = body.find("h1", {"id": "firstHeading"}).string

      # get the first paragraph
      strings = []
      try:
        for node in response.xpath('//*[@id="mw-content-text"]/div/p'):
          text = text_cleaner(node.xpath('string()').extract())
          if len(text):
            strings.append(text)
      except Exception as error:
        strings.append(str(error))

      item['text'] = ' '.join(strings)

      data = [
          item['cat'],
          item['title'],
          item['url'],
          item['text']
      ]

      # don't upload empty or broken data
      if not None in data:
        global cursor
        hana_upload(cursor, data)
        print(f' -> Upload: {data[0]} > {data[1]} > {data[1]}')
        # return for scrapy
        yield item

    # else don't do anything
    else:
      print(f'Page is useless: {response.url}')
      pass
