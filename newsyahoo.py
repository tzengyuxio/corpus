#!/usr/bin/env python3
"""Crawler of Books.com
"""

import json
import logging
import re
import sqlite3
import sys
from random import randint
from time import sleep
from urllib.error import HTTPError
from urllib.request import Request, urljoin, urlopen

from bs4 import BeautifulSoup

from utils import PARSER, datetime_iso, is_unihan

SQL_CREATE_TABLE_ARTICLES = '''
CREATE TABLE IF NOT EXISTS articles (
    id TEXT, author TEXT, publisher TEXT, pub_date TEXT, url TEXT, title TEXT, article TEXT,
    PRIMARY KEY(id)
)
'''
SQL_CREATE_TABLE_TODAY_PICKS = '''
CREATE TABLE IF NOT EXISTS today_picks (
    id TEXT, title TEXT, url TEXT, pick_links TEXT,
    PRIMARY KEY(id)
)
'''
SQL_CONTAIN_ARTICLE = '''
SELECT 1 FROM articles WHERE url=?
'''
SQL_SELECT_TODAY_PICKS = '''
SELECT id, pick_links FROM today_picks
'''
SQL_INSERT_ARTICLE = '''
INSERT OR IGNORE INTO articles (id, author, publisher, pub_date, url, title, article) VALUES (?, ?, ?, ?, ?, ?, ?)
'''
SQL_INSERT_TODAY_PICK = '''
INSERT OR IGNORE INTO today_picks (id, title, url, pick_links) VALUES (?, ?, ?, ?)
'''

URL_NEWS_YAHOO = 'https://tw.news.yahoo.com'
URL_YAHOO_TODAY = 'https://tw.news.yahoo.com/topic/yahoo-today'
URL_INDEXDATASERVICE_PATH = '/_td-news/api/resource/IndexDataService.getEditorialList;loadMore=true;count={0};start={1};mrs=%7B%22size%22%3A%7B%22w%22%3A220%2C%22h%22%3A128%7D%7D;uuid=f1d5a047-b405-4a6b-992b-f5298db387f5?'
URL_INDEXDATASERVICE = URL_NEWS_YAHOO + URL_INDEXDATASERVICE_PATH


SQL_INSERT_NEWS_TODAY_PICKS = '''
INSERT OR IGNORE INTO today_picks (id, title, url) VALUES (?, ?, ?)
'''
SQL_UPDATE_NEWS_TODAY_PICKS = '''
UPDATE today_picks SET links=? WHERE id=?
'''
SQL_INSERT_ARTICLES = '''
INSERT OR IGNORE INTO articles (id, title, provider, published, url, cont) VALUES (?, ?, ?, ?, ?, ?)
'''
SQL_SELECT_NEWS_TODAY_PICKS = '''
SELECT * FROM today_picks
'''
SQL_INSERT_CORPUS = '''
INSERT OR IGNORE INTO corpus VALUES (?, ?, ?, ?, ?, ?, ?)
'''


class NewsYahooCrawler():
    """Crawler of news yahoo
    """

    def __init__(self):
        # self.writer = writer
        self.urlopen_count = 0
        self.init_db()
        self.init_logger()
        self.logger.info('---- [NewsYahooCrawler] ---------------------------')

    def init_db(self):
        """init db
        """
        self.conn = sqlite3.connect('source-newsyahoo.db')
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_TABLE_ARTICLES)
        cur.execute(SQL_CREATE_TABLE_TODAY_PICKS)
        self.conn.commit()
        cur.close()

    def init_logger(self):
        """init logger
        """
        self.logger = logging.getLogger('newsyahoo')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-7s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        ifh = logging.FileHandler('crawler-newsyahoo.log', encoding='utf-8')
        ifh.setFormatter(formatter)
        ifh.setLevel(logging.INFO)
        wfh = logging.FileHandler(
            'crawler-newsyahoo-warn.log', encoding='utf-8')
        wfh.setFormatter(formatter)
        wfh.setLevel(logging.WARN)
        dsh = logging.StreamHandler()
        dsh.setFormatter(formatter)
        dsh.setLevel(logging.DEBUG)

        self.logger.addHandler(ifh)
        self.logger.addHandler(wfh)
        self.logger.addHandler(dsh)

    def sleep(self):
        """sleep
        """
        self.urlopen_count += 1
        if self.urlopen_count % 3 == 0:
            sleep(randint(2, 4))

    def contain_article(self, link):
        """contain_article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_CONTAIN_ARTICLE, [link])
        result = True if cur.fetchone() is not None else False
        cur.close()
        return result

    def insert_today_picks(self, today_picks):
        """insert_today_picks
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_TODAY_PICK, today_picks)
        self.conn.commit()
        cur.close()

    def insert_article(self, article_values):
        """insert article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_ARTICLE, article_values)
        self.conn.commit()
        cur.close()

    def fetch_daily_summary_urls(self):
        """fetch_daily_summary_urls
        """
        req = Request(url=URL_INDEXDATASERVICE)
        count = 30  # most 30
        start = 0
        daily_summary_urls = []
        while True:
            req = Request(url=URL_INDEXDATASERVICE.format(count, start))
            with urlopen(req) as furl:
                if furl.getcode() != 200:
                    self.logger.error('fetch error when start=%d', start)
                    break
                data = json.loads(furl.read().decode('utf-8'))
                if data is None or len(data) == 0:
                    self.logger.error('fetch nothing when start=%d', start)
                    break

                self.logger.info('fetch %d today picks from start=%d',
                                 len(data), start)
                for item in data:
                    summary = [item['id'], item['title'], item['url']]
                    daily_summary_urls.append(summary)
                start += len(data)
        return daily_summary_urls

    def fetch_today_picks(self, daily_summary_urls):
        """fetch_today_picks
        """
        for summary in daily_summary_urls:
            self.logger.info('fetching today_picks [%s](%s)...',
                             summary[1], summary[0])
            self.sleep()
            url = urljoin(URL_NEWS_YAHOO, summary[2])
            soup = BeautifulSoup(urlopen(url), PARSER)
            hrefs = []
            for elem in soup(text=re.compile(r'詳全文')):
                if elem.parent.name == 'a':
                    hrefs.append(elem.parent['href'])
            soup.decompose()
            hrefs_json = json.dumps(hrefs)
            summary.append(hrefs_json)
            self.insert_today_picks(summary)

    def fetch_article(self, url):
        """fetch_article
        """
        self.logger.info('fetching article(%s)...', url)
        if self.contain_article(url):
            self.logger.info('      -> already saved link: %s', url)
            return
        self.sleep()
        try:
            soup = BeautifulSoup(urlopen(url), PARSER)
            title = soup.find('header').text
            art = soup.find('article').text
            uuid = soup.find('article')['data-uuid']
            pub_date = soup.find('time')['datetime'][:10]
            author_tag = soup.find('div', {'class': 'author'})
            provdr_tag = soup.find('span', {'class': 'provider-link'})
            author = author_tag.text if author_tag != None else ''
            provider = provdr_tag.text if provdr_tag != None else ''
            soup.decompose()
            article_values = [uuid, author, provider,
                              pub_date, url, title, art]
            self.insert_article(article_values)
            self.logger.info(
                '      -> [%s] by "%s|%s" at %s', title, provider, author, pub_date)
        except HTTPError as err:
            self.logger.error(
                '      -> article(%s) fetch fail: %s', url, err)
        except KeyError as err:
            self.logger.error(
                '      -> article(%s) fetch fail: KeyError: %s', url, err)
        except AttributeError as err:
            self.logger.error(
                '      -> article(%s) fetch fail: AttributeError: %s', url, err)

    def fetch_articles(self):
        """fetch_articles
        """
        cur = self.conn.cursor()
        for row in cur.execute(SQL_SELECT_TODAY_PICKS):
            pick_links = json.loads(row[1])
            for url in pick_links:
                self.fetch_article(url)

    def calc_all(self):
        """calc_all
        """
        # corpus (src TEXT, idx TEXT, raw_text TEXT, stats TEXT, num_char
        # INTEGER, num_hanzi INTEGER, num_unique INTEGER,
        cur = self.conn.cursor()
        for row in cur.execute('SELECT * FROM articles'):
            src = 'newsyahoo'
            idx = row[0]
            raw_text = '{0}\n{1}'.format(row[1], row[5])
            trimed_text = raw_text.replace(' ', '').replace('\n', '')
            num_char = len(trimed_text)
            char_freq_table = {}
            for char in trimed_text:
                if char in char_freq_table:
                    char_freq_table[char] += 1
                else:
                    char_freq_table[char] = 1
            char_freq_table = {k: v for k,
                               v in char_freq_table.items() if is_unihan(k)}
            num_hanzi = sum(char_freq_table.values())
            num_unique = len(char_freq_table)
            stats = json.dumps(
                char_freq_table, ensure_ascii=False, sort_keys=True).encode('utf-8')
            # cur_ins = self.corpus.cursor()
            # cur_ins.execute(SQL_INSERT_CORPUS, (src, idx, raw_text,
            # stats, num_char, num_hanzi, num_unique))
            print('{0} [INFO] Calc news[{1}] ... num(char/hanzi/unique) = {2}/{3}/{4}'.format(
                datetime_iso(), row[0], num_char, num_hanzi, num_unique))
            # cur_ins.close()
        cur.close()
        # self.corpus.commit()

    def fetch_all(self):
        """fetch_all
        """
        daily_summary_urls = self.fetch_daily_summary_urls()
        self.fetch_today_picks(daily_summary_urls)
        self.fetch_articles()


def print_usage():
    """Print Usage
    """
    print('usage: {0} command'.format(sys.argv[0]))
    print('')
    print('    fetch   fetch ')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(0)
    elif sys.argv[1] == 'all':
        CRAWLER = NewsYahooCrawler()
        CRAWLER.fetch_all()
    elif sys.argv[1] == 'fetch':
        NEWS = NewsYahooCrawler()
        NEWS.fetch_all()
    elif sys.argv[1] == 'calc':
        NEWS = NewsYahooCrawler()
        NEWS.calc_all()
    elif sys.argv[1] == 'test':
        CRAWLER = NewsYahooCrawler()
        # CRAWLER.crawl_article()
        # CRAWLER.crawl_month(2016, 5, 3, 20)
