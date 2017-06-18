#!/usr/bin/env python3
"""Crawler of Books.com
"""

import calendar
import json
import sqlite3
import sys
from datetime import datetime
from random import randint
from time import sleep
from urllib.request import Request, urljoin, urlopen

from bs4 import BeautifulSoup

PARSER = 'html.parser'

NEWARTICLE = 'http://mag.cnyes.com/WebService/WebAjaxSvr.asmx/NewArticle'

SQL_CREATE_MAGCNYES_RANKINGS = '''
CREATE TABLE IF NOT EXISTS
    rankings (year INTEGER, month INTEGER, col_id INTEGER, top INTEGER,
    col_name TEXT, art_id TEXT, title TEXT, full_title TEXT, mag_name TEXT, url TEXT,
    PRIMARY KEY(year, month, col_id, top))
'''
SQL_CREATE_MAGCNYES_ARTICLES = '''
CREATE TABLE IF NOT EXISTS
    articles (art_id TEXT, col_id INTEGER, col_name TEXT, title TEXT, full_title TEXT, mag_name TEXT, url TEXT, cont TEXT,
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
SQL_INSERT_MAGCNYES_ARTICLES = '''
INSERT OR IGNORE INTO articles VALUES (?, ?, ?, ?, ?, ?, ?, ?)
'''
SQL_INSERT_CORPUS = '''
INSERT OR IGNORE INTO corpus VALUES (?, ?, ?, ?, ?, ?, ?)
'''
SQL_EXISTS_ARTICLE = '''
SELECT 1 FROM articles WHERE art_id=?
'''
SQL_SELECT_BOOKS = '''
SELECT * FROM books WHERE page_count > 0
'''


def datetime_iso():
    """datetime_iso
    """
    return datetime.now().replace(microsecond=0).isoformat(' ')


def is_unihan(char):
    """check a char is hanzi or not
    """
    return ('\u4e00' <= char <= '\u9fff' or
            is_unihan_ext(char))


def is_unihan_ext(char):
    """
    CJK Unified Ideographs Extension A: '\U00003400' <= char <= '\U00004dbf'
    CJK Unified Ideographs Extension B: '\U00020000' <= char <= '\U0002a6df'
    CJK Unified Ideographs Extension C: '\U0002a700' <= char <= '\U0002b73f'
    CJK Unified Ideographs Extension D: '\U0002b740' <= char <= '\U0002b81f'
    CJK Unified Ideographs Extension E: '\U0002b820' <= char <= '\U0002ceaf'
    """
    return ('\u3400' <= char <= '\u4dbf' or  # CJK Unified Ideographs Extension A
            '\U00020000' <= char <= '\U0002a6df' or  # CJK Unified Ideographs Extension B
            '\U0002a700' <= char <= '\U0002ceaf')  # CJK Unified Ideographs Extension C,D,E


def month_range(year, month):
    """month_range
    """
    first, last = calendar.monthrange(year, month)
    first_date = '{0:04}-{1:02}-{2:02}'.format(year, month, first)
    last_date = '{0:04}-{1:02}-{2:02}'.format(year, month, last)
    return first_date, last_date


class SqliteWriter():
    """SQLite Writer
    """

    def __init__(self):
        self.conn = sqlite3.connect('magcnyes.db')
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_MAGCNYES_RANKINGS)
        cur.execute(SQL_CREATE_MAGCNYES_ARTICLES)
        self.conn.commit()
        cur.close()

        self.corpus = sqlite3.connect('corpus.db')
        cur = self.corpus.cursor()
        cur.execute(SQL_CREATE_CORPUS)
        self.corpus.commit()
        cur.close()

    def write_ranking(self, values):
        """write_ranking
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_MAGCNYES_RANKINGS, values)
        self.conn.commit()
        cur.close()

    def write_article(self, values):
        """write_article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_MAGCNYES_ARTICLES, values)
        self.conn.commit()
        cur.close()

    def contains_article(self, art_id):
        """contains_book
        """
        cur = self.conn.cursor()
        cur.execute(SQL_EXISTS_ARTICLE, [art_id])
        self.conn.commit()
        result = True if cur.fetchone() is not None else False
        cur.close()
        return result

    def select_book(self):
        """select_book
        """
        cur = self.conn.cursor()
        for row in cur.execute(SQL_SELECT_BOOKS):
            src = 'books'
            idx = row[0]
            raw_text = '{0}\n\n{1}\n{2}'.format(row[1], row[2], row[4])
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
            stats = json.dumps(char_freq_table, sort_keys=True)
            cur_ins = self.conn.cursor()
            cur_ins.execute(SQL_INSERT_CORPUS, (src, idx, raw_text,
                                                stats, num_char, num_hanzi, num_unique))
            print('{0} [INFO] Calc book[{1}] ... num(char/hanzi/unique) = {2}/{3}/{4}'.format(
                datetime_iso(), row[0], num_char, num_hanzi, num_unique))
            self.conn.commit()
            cur_ins.close()
        cur.close()


class MagCnyes():
    """Crawler of MagCnyes
    """

    def __init__(self, writer):
        self.writer = writer
        self.urlopen_count = 0
        self.columns = {1: u'時尚', 2: u'生活', 7: u'醫美', 8: u'旅遊',
                        9: u'藝文', 10: u'設計', 3: u'商業', 5: u'理財', 6: u'科技'}

    def sleep(self):
        """sleep
        """
        self.urlopen_count += 1
        if self.urlopen_count % 100 == 0:
            for _ in range(0, randint(30, 90), 2):
                print('=', end='', flush=True)
                sleep(2)
        elif self.urlopen_count % 10 == 0:
            for _ in range(randint(5, 9)):
                print('-', end='', flush=True)
                sleep(1)
        else:
            sleep(randint(1, 3))

    def fetch_article(self, art_id, col_id, title, full_title, mag_name, url_first):
        """fetch_article
        """
        print('{0} [INFO]   Fetching Article: {1}'.format(
            datetime_iso(), title), end='', flush=True)
        if self.writer.contains_article(art_id):
            print(' -> contained and skip')
            return

        text = ''
        url_base = urljoin(NEWARTICLE, url_first)
        url_page = url_first
        while url_page is not None:
            print('.', end='', flush=True)
            # self.sleep()
            url = urljoin(url_base, url_page)
            soup = BeautifulSoup(urlopen(url), PARSER)
            text += soup.find_all('div', {'class': 'content'})[0].text
            bnext_btns = soup.find_all('a', {'class': 'bnext'})
            url_page = None if len(
                bnext_btns) == 0 else bnext_btns[0].get('href')
            soup.decompose()
        self.sleep()

        # write
        self.writer.write_article(
            [art_id, col_id, self.columns[col_id], title, full_title, mag_name, url_first, text])
        print('saved')
        return

    def fetch_month(self, year, month, col_id):
        """fetch_mont
        """
        print('{0} [INFO] Processing Top100 of {1}-{2:02} at {3}'.format(
            datetime_iso(), year, month, self.columns[col_id]), end='', flush=True)
        page_size = 100
        start_date, end_date = month_range(year, month)
        req_body_json = '{{"Start":"{2}","End":"{3}","ColumnID":{0},"PageSize":{1},"PageIndex":1}}'
        req_data = req_body_json.format(
            col_id, page_size, start_date, end_date)
        req = Request(url=NEWARTICLE, data=req_data.encode(encoding='utf_8'))
        req.add_header('Content-Type', 'application/json')
        with urlopen(req) as resp:
            data = json.loads(resp.read().decode('utf-8'))['d']
            if data['List'] is None:
                print(' -> No List')
                return
            print(' -> {0} articles'.format(len(data['List'])))
            for idx, art in enumerate(data['List']):
                art_id = art['ArticleID']
                full_title = art['FullTitle']
                mag_name = art['MagName']
                title = art['Title']
                url = art['Url']
                self.writer.write_ranking(
                    [year, month, col_id, idx + 1, self.columns[col_id],
                     art_id, title, full_title, mag_name, url])
                self.fetch_article(art_id, col_id, title,
                                   full_title, mag_name, url)

    def fetch_all(self):
        """fetch_all
        """
        start = (2013, 7)
        end = (2016, 12)
        for year in range(start[0], end[0] + 1):
            start_month = start[1] if year == start[0] else 1
            end_month = end[1] if year == end[0] else 13
            for month in range(start_month, end_month):
                for col_id in self.columns:
                    self.fetch_month(year, month, col_id)

    def calc_all(self):
        """calc_all
        """
        self.writer.select_book()

    def calc_one(self):
        """calc_one
        """


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
    elif sys.argv[1] == 'fetch':
        WRITER = SqliteWriter()
        MAG = MagCnyes(WRITER)
        MAG.fetch_all()
        # MAG.fetch_month(2017, 4, 7)
        # print(BOOK.test_book('0010723234'))
        # print(BOOK.fetch_book('0010723234', '為了活下去：脫北女孩朴研美', '朴研美', 5))
        # BOOK.fetch_book('0010723234', '為了活下去：脫北女孩朴研美', '朴研美')
    elif sys.argv[1] == 'calc':
        WRITER = SqliteWriter()
        MAG = MagCnyes(WRITER)
        MAG.calc_all()
