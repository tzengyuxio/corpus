#!/usr/bin/env python3
"""Crawler of Books.com
"""

import json
import sqlite3
import sys
import re
from random import randint
from time import sleep
from urllib.request import Request, urljoin, urlopen
from bs4 import BeautifulSoup
from utils import datetime_iso, is_unihan, month_range

PARSER = 'html.parser'
YAHOO_TODAY = 'https://tw.news.yahoo.com/topic/yahoo-today'
INDEX_DATA_SERVICE_PATH = '/_td-news/api/resource/IndexDataService.getEditorialList;loadMore=true;count={0};start={1};mrs=%7B%22size%22%3A%7B%22w%22%3A220%2C%22h%22%3A128%7D%7D;uuid=f1d5a047-b405-4a6b-992b-f5298db387f5?'
INDEX_DATA_SERVICE = 'https://tw.news.yahoo.com' + INDEX_DATA_SERVICE_PATH


SQL_CREATE_NEWS_TODAY_PICKS = '''
CREATE TABLE IF NOT EXISTS
    today_picks (id TEXT, title TEXT, url TEXT, links TEXT,
    PRIMARY KEY(id))
'''
SQL_CREATE_NEWS_ARTICLES = '''
CREATE TABLE IF NOT EXISTS
    articles(id TEXT, title TEXT, provider TEXT, published TEXT, url TEXT, cont TEXT,
    PRIMARY KEY(id))
'''
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


class News():
    """Crawler of news yahoo
    """

    def __init__(self):
        # self.writer = writer
        self.urlopen_count = 0
        self.init_db()

    def sleep(self):
        """sleep
        """
        self.urlopen_count += 1
        sleep(randint(1, 3))
        # if self.urlopen_count % 100 == 0:
        #     for _ in range(0, randint(24, 96), 2):
        #         print('=', end='', flush=True)
        #         sleep(2)
        # elif self.urlopen_count % 10 == 0:
        #     for _ in range(randint(12, 20)):
        #         print('-', end='', flush=True)
        #         sleep(1)
        # else:
        #     sleep(randint(3, 5))

    def init_db(self):
        """init_db
        """
        self.conn = sqlite3.connect('newsyahoo.db')
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_NEWS_TODAY_PICKS)
        cur.execute(SQL_CREATE_NEWS_ARTICLES)
        self.conn.commit()
        cur.close()

    def fetch_list(self):
        """fetch_list
        """
        req = Request(url=INDEX_DATA_SERVICE)
        count = 30  # most 30
        start = 0
        while True:
            req = Request(url=INDEX_DATA_SERVICE.format(count, start))
            with urlopen(req) as f:
                if f.getcode() != 200:
                    print('[ERROR] fetch error when start={0}'.format(start))
                    break
                data = json.loads(f.read().decode('utf-8'))
                if data is None or len(data) == 0:
                    print('[ERROR] fetch nothing when start={0}'.format(start))
                    # print(f.read())
                    break
                print('[INFO] fetch {0} today picks from start={1}'.format(
                    len(data), start))
                for item in data:
                    cur = self.conn.cursor()
                    cur.execute(SQL_INSERT_NEWS_TODAY_PICKS,
                                (item['id'], item['title'], item['url']))
                    self.conn.commit()
                    cur.close()
                start += len(data)

    def fetch_picks(self):
        """fetch_picks
        """
        cur = self.conn.cursor()
        for row in cur.execute(SQL_SELECT_NEWS_TODAY_PICKS):
            url = urljoin('https://tw.news.yahoo.com', row[2])
            print(url)
            self.sleep()
            soup = BeautifulSoup(urlopen(url), PARSER)
            links = []
            for elem in soup(text=re.compile(r'詳全文')):
                if elem.parent.name == 'a':
                    print('[INFO]    {0}'.format(elem.parent['href']))
                    links.append(elem.parent['href'])
            links_str = json.dumps(links)
            cur_daily = self.conn.cursor()
            cur_daily.execute(SQL_UPDATE_NEWS_TODAY_PICKS, (links_str, row[0]))
            self.conn.commit()
            cur_daily.close()
            soup.decompose()

    def has_link(self, link):
        """has_link
        """
        cur = self.conn.cursor()
        cur.execute('SELECT 1 FROM articles WHERE url=?', [link])
        self.conn.commit()
        result = True if cur.fetchone() is not None else False
        cur.close()
        return result

    def fetch_articles(self):
        """fetch_articles
        """
        cur = self.conn.cursor()
        for row in cur.execute('SELECT id, links FROM today_picks'):
            links = json.loads(row[1])
            for link in links:
                if self.has_link(link):
                    print('{0} [INFO] already fetched link; {1} '.format(datetime_iso(), link))
                    continue
                self.sleep()
                print('{0} [INFO] fetching...{1} '.format(datetime_iso(), link), end='', flush=True)
                resp = urlopen(link)
                if resp.getcode() != 200:
                    print('-> ERROR, code={0}'.format(resp.getcode))
                    continue
                soup = BeautifulSoup(resp, PARSER)
                url = link
                title = soup.find('header').text
                art = soup.find('article').text
                uuid = soup.find('article')['data-uuid']
                date = soup.find('time')['datetime'][:10]
                provider_elem = soup.find('span', {'class': 'provider-link'})
                author_elem = soup.find('div', {'class': 'author'})
                author = ""
                if author_elem != None:
                    author = author_elem.text
                elif provider_elem != None:
                    author = provider_elem.text
                print('-> {2} | {1}: {0}'.format(title, author, date))
                cur_art = self.conn.cursor()
                cur_art.execute(SQL_INSERT_ARTICLES,
                                (uuid, title, author, date, url, art))
                self.conn.commit()
                cur_art.close()
                soup.decompose()


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
    elif sys.argv[1] == 'list':
        NEWS = News()
        NEWS.fetch_list()
        NEWS.fetch_picks()
    elif sys.argv[1] == 'fetch':
        NEWS = News()
        NEWS.fetch_articles()
    elif sys.argv[1] == 'calc':
        pass
        # WRITER = SqliteWriter()
        # BOOK = Books(WRITER)
        # BOOK.calc_all()
    elif sys.argv[1] == 'date':
        pass
        # WRITER = SqliteWriter()
        # BOOK = Books(WRITER)
        # BOOK.insert_published()
        # CONTAINS, DATE = BOOK.book_info('0010592120')
        # CONTAINS, DATE = BOOK.book_info('0010592301')
        # print(CONTAINS)
        # print(DATE)
