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


class WikiSpider(CrawlSpider):
  """
  the Page Spider for wikipedia
  """

  name = "wiki_all"
  allowed_domains = ["en.wikipedia.org"]

  start_urls = [
      "https://en.wikipedia.org/wiki/Main_Page"
  ]
  base_url = 'https://en.wikipedia.org'

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
              "https://en\.wikipedia\.org/wiki/Category.*",
              "https://en\.wikipedia\.org/wiki/File:.*",
              "https://en\.wikipedia\.org/wiki/Wikipedia:.*"
          ]),
          callback='parse_wiki'),
  )

  def hana_init(ip, port, user, pw):
    connection = dbapi.connect(ip, port, user, pw)
    cursor = connection.cursor()
    return cursor

  global cursor
  cursor = hana_init('34.244.52.114', 39015, 'SYSTEM', 'Glorp2018!')

  def parse_wiki(self, response):
    def hana_upload(cursor, data):
      url = data[0].replace("'", "''")
      title = data[1].replace("'", "''")
      text = data[2].replace("'", "''")
      sql = f"insert into \"SYSTEM\".\"WIKI\" (TITLE, TEXT, URL) VALUES ('{title}','{text}','{url}')"
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

    item = WikiItem()
    body = BeautifulSoup(response.body)

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
        item['url'],
        item['title'],
        item['text']
    ]
    # don't upload empty or broken data
    if data[0] is not None and data[1] is not None and data[2] is not None:
      hana_upload(cursor, data)
      # print(data[2])

    # load new pages
    base_url = self.base_url
    if response.url.startswith(base_url):
      links = response.xpath("//a/@href").extract()
      regex = re.compile(r'^/wiki/.*')
      selected_links = list(filter(regex.search, links))
      for link in selected_links:
        if ':' not in link:
          # print(link)
          absolute_next_page_url = base_url + link
          yield Request(absolute_next_page_url)

    return item
