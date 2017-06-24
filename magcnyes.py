#!/usr/bin/env python3
"""Crawler of Mag Cnyes.com
"""
import json
import logging
import sqlite3
import sys
from random import randint
from time import sleep
from urllib.request import Request, urljoin, urlopen

from bs4 import BeautifulSoup

from utils import PARSER, datetime_iso, is_unihan, month_range

SQL_CREATE_TABLE_ARTICLES = '''
CREATE TABLE IF NOT EXISTS articles (
    art_id TEXT, col_id INTEGER, col_name TEXT, publisher TEXT, pub_date TEXT, title TEXT, full_title TEXT, article TEXT,
    PRIMARY KEY(art_id)
)
'''
SQL_CREATE_TABLE_RANKINGS = '''
CREATE TABLE IF NOT EXISTS rankings (
    year INTEGER, month INTEGER, col_id INTEGER, ranking INTEGER,
    col_name TEXT, art_id TEXT, title TEXT, full_title TEXT, mag_name TEXT, url TEXT,
    PRIMARY KEY(year, month, col_id, ranking)
)
'''
SQL_CONTAIN_ARTICLE = '''
SELECT 1 FROM articles WHERE art_id=?
'''
SQL_INSERT_ARTICLE = '''
INSERT OR IGNORE INTO articles
    (art_id, col_id, col_name, publisher, pub_date, title, full_title, article) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
'''
SQL_INSERT_RANKING = '''
INSERT OR IGNORE INTO rankings
    (year, month, col_id, ranking, col_name, art_id, title, full_title, mag_name, url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

URL_NEWARTICLE = 'http://mag.cnyes.com/WebService/WebAjaxSvr.asmx/NewArticle'

SQL_CREATE_MAGCNYES_RANKINGS = '''
CREATE TABLE IF NOT EXISTS
    rankings (year INTEGER, month INTEGER, col_id INTEGER, top INTEGER,
    col_name TEXT, art_id TEXT, title TEXT, full_title TEXT, mag_name TEXT, url TEXT,
    PRIMARY KEY(year, month, col_id, top))
'''
SQL_CREATE_MAGCNYES_ARTICLES = '''
CREATE TABLE IF NOT EXISTS
    articles (art_id TEXT, col_id INTEGER, col_name TEXT, title TEXT, full_title TEXT, mag_name TEXT, published TEXT, url TEXT, cont TEXT,
    PRIMARY KEY(art_id))
'''
# num_char:   number of character in raw_text except space and new-line
# num_hanzi:  number of hanzi in raw_text
# num_unique: number of unique hanze in raw_text
SQL_CREATE_CORPUS = '''
CREATE TABLE IF NOT EXISTS
    corpus (src TEXT, idx TEXT, raw_text TEXT, stats TEXT, num_char INTEGER, num_hanzi INTEGER, num_unique INTEGER,
    PRIMARY KEY(src, idx))
'''
SQL_INSERT_MAGCNYES_RANKINGS = '''
INSERT OR IGNORE INTO rankings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''
SQL_INSERT_CORPUS = '''
INSERT OR IGNORE INTO corpus VALUES (?, ?, ?, ?, ?, ?, ?)
'''
SQL_SELECT_ARTICLES = '''
SELECT art_id, full_title, cont FROM articles WHERE substr(published, 0, 5)="2016"
'''


class SqliteWriter():
    """SQLite Writer
    """

    def __init__(self):
        self.conn = sqlite3.connect('magcnyes.db')
        # cur = self.conn.cursor()
        # cur.execute(SQL_CREATE_MAGCNYES_RANKINGS)
        # cur.execute(SQL_CREATE_MAGCNYES_ARTICLES)
        # self.conn.commit()
        # cur.close()

    def write_ranking(self, values):
        """write_ranking
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_MAGCNYES_RANKINGS, values)
        self.conn.commit()
        cur.close()

    def select_articles(self):
        """select_book
        """
        cur = self.conn.cursor()
        for row in cur.execute(SQL_SELECT_ARTICLES):
            src = 'magcnyes'
            idx = row[0]
            raw_text = '{0}\n\n{1}'.format(row[1], row[2])
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
            print('{0} [INFO] Calc article[{1}] ... num(char/hanzi/unique) = {2}/{3}/{4}'.format(
                datetime_iso(), row[0], num_char, num_hanzi, num_unique))
            # cur_ins.close()
        cur.close()
        # self.corpus.commit()


class MagCnyesCrawler():
    """Crawler of MagCnyes
    """

    def __init__(self, writer):
        self.writer = writer
        self.urlopen_count = 0
        self.columns = {1: u'時尚', 2: u'生活', 7: u'醫美', 8: u'旅遊',
                        9: u'藝文', 10: u'設計', 3: u'商業', 5: u'理財', 6: u'科技'}
        self.init_db()
        self.init_logger()
        self.logger.info('---- [MagCnyesCrawler] ---------------------------')

    def init_db(self):
        """init db
        """
        self.conn = sqlite3.connect('source-magcnyes.db')
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_TABLE_ARTICLES)
        cur.execute(SQL_CREATE_TABLE_RANKINGS)
        self.conn.commit()
        cur.close()

    def init_logger(self):
        """init logger
        """
        self.logger = logging.getLogger('magcnyes')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-7s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        ifh = logging.FileHandler('crawler-magcnyes.log', encoding='utf-8')
        ifh.setFormatter(formatter)
        ifh.setLevel(logging.INFO)
        wfh = logging.FileHandler(
            'crawler-magcnyes-warn.log', encoding='utf-8')
        wfh.setFormatter(formatter)
        wfh.setLevel(logging.WARN)
        dsh = logging.StreamHandler()
        dsh.setFormatter(formatter)
        dsh.setLevel(logging.DEBUG)

        self.logger.addHandler(ifh)
        self.logger.addHandler(wfh)
        self.logger.addHandler(dsh)

    def sleep(self, sec=None):
        """sleep
        """
        self.urlopen_count += 1
        if sec is not None:
            sleep(sec)
            return
        if self.urlopen_count % 100 == 0:
            seconds = randint(30, 90)
            print('(count: {0}, sleep {1} seconds) ['.format(
                self.urlopen_count, seconds), end='')
            for _ in range(0, seconds, 2):
                print('=', end='', flush=True)
                sleep(2)
            print(']')
        elif self.urlopen_count % 10 == 0:
            seconds = randint(5, 9)
            print('(count: {0}, sleep {1} seconds) ['.format(
                self.urlopen_count, seconds), end='')
            for _ in range(seconds):
                print('-', end='', flush=True)
                sleep(1)
            print(']')
        else:
            sleep(randint(1, 3))

    def contain_article(self, art_id):
        """check if contains article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_CONTAIN_ARTICLE, [art_id])
        result = True if cur.fetchone() is not None else False
        cur.close()
        return result

    def insert_article(self, article_values):
        """insert article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_ARTICLE, article_values)
        self.conn.commit()
        cur.close()

    def insert_ranking(self, ranking_values):
        """insert ranking
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_RANKING, ranking_values)
        self.conn.commit()
        cur.close()

    def crawl_article(self, art_id, col_id, title, full_title, mag_name, url_first):
        """crawl_article
        """
        self.logger.info('fetching article[%s] %s...', art_id, title)

        if mag_name in ('TAIPEI', 'Discover Taipei'):
            self.logger.info(
                '      -> article[%s] in foreign language magazine (%s)', art_id, mag_name)
            return

        if self.contain_article(art_id):
            self.logger.info('      -> article[%s] contained and skip', art_id)
            return

        cont = ''
        page_cnt = 0
        url_base = urljoin(URL_NEWARTICLE, url_first)
        url_page = url_first
        while url_page is not None:
            self.sleep(sec=0)
            page_cnt += 1
            url = urljoin(url_base, url_page)
            soup = BeautifulSoup(urlopen(url), PARSER)
            div_contents = soup.find_all('div', {'class': 'content'})
            if div_contents is None or len(div_contents) == 0:
                self.logger.warning(
                    '      -> article[%s] content broken', art_id)
                return
            cont += div_contents[0].text
            bnext_btns = soup.find_all('a', {'class': 'bnext'})
            if len(bnext_btns) == 0:
                url_page = None
            else:
                url_page = bnext_btns[0].get('href')
            soup.decompose()

        pub_date = '{0}-{1}-{2}'.format(url_first[9:13],
                                        url_first[13:15], url_first[15:17])
        article_values = [art_id, col_id, self.columns[col_id],
                          mag_name, pub_date, title, full_title, cont]
        self.insert_article(article_values)
        self.logger.info(
            '      -> article[%s] %s %d paged saved', art_id, title, page_cnt)

    def crawl_month(self, year, month, col_id, page_size):
        """crawl_month
        """
        self.logger.info('fetching TOP 100 of %4d-%02d at %s',
                         year, month, self.columns[col_id])
        start_date, end_date = month_range(year, month)
        req_body_json = '{{"Start":"{2}","End":"{3}","ColumnID":{0},"PageSize":{1},"PageIndex":1}}'
        data = req_body_json.format(col_id, page_size, start_date, end_date)
        req = Request(url=URL_NEWARTICLE, data=data.encode(encoding='utf_8'))
        req.add_header('Content-Type', 'application/json')
        with urlopen(req) as resp:
            data = json.loads(resp.read().decode('utf-8'))['d']
            if data['List'] is None:
                self.logger.info('      -> No List')
                return
            self.logger.info('      -> %d articles', len(data['List']))
            for idx, art in enumerate(data['List']):
                art_id = art['ArticleID']
                full_title = art['FullTitle']
                mag_name = art['MagName']
                title = art['Title']
                url = art['Url']
                self.insert_ranking(
                    [year, month, col_id, idx + 1, self.columns[col_id],
                     art_id, title, full_title, mag_name, url])
                self.crawl_article(art_id, col_id, title,
                                   full_title, mag_name, url)

    def fetch_all(self):
        """fetch_all
        """
        start = (2013, 7)
        end = (2017, 6)
        page_size = 100
        for year in range(start[0], end[0] + 1):
            start_month = start[1] if year == start[0] else 1
            end_month = end[1] if year == end[0] else 13
            for month in range(start_month, end_month):
                for col_id in self.columns:
                    self.crawl_month(year, month, col_id, page_size)

    def calc_all(self):
        """calc_all
        """
        self.writer.select_articles()


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
        WRITER = SqliteWriter()
        CRAWLER = MagCnyesCrawler(WRITER)
        CRAWLER.fetch_all()
    elif sys.argv[1] == 'fetch':
        WRITER = SqliteWriter()
        MAG = MagCnyesCrawler(WRITER)
        MAG.fetch_all()
        # MAG.fetch_month(2017, 4, 7)
    elif sys.argv[1] == 'calc':
        WRITER = SqliteWriter()
        MAG = MagCnyesCrawler(WRITER)
        MAG.calc_all()
    elif sys.argv[1] == 'test':
        WRITER = SqliteWriter()
        CRAWLER = MagCnyesCrawler(WRITER)
        # CRAWLER.crawl_article()
        CRAWLER.crawl_month(2016, 5, 3, 20)
