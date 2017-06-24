#!/usr/bin/env python3
"""Crawler of Books.com
"""
import json
import logging
import sqlite3
import sys
from random import randint
from time import sleep, time
from urllib.error import HTTPError
from urllib.request import urlopen

from bs4 import BeautifulSoup

from utils import PARSER, datetime_iso, is_unihan

SQL_CREATE_TABLE_ARTICLES = '''
CREATE TABLE IF NOT EXISTS articles (
    book_no TEXT, isbn TEXT, author TEXT, publisher TEXT, pub_date TEXT, title TEXT, article TEXT,
    PRIMARY KEY(book_no)
)
'''
SQL_CREATE_TABLE_BOOKS = '''
CREATE TABLE IF NOT EXISTS books (
    book_no TEXT, page_cnt INTEGER,
    PRIMARY KEY(book_no)
)
'''
SQL_CREATE_TABLE_RANKINGS = '''
CREATE TABLE IF NOT EXISTS rankings (
    year INTEGER, month INTEGER, ranking INTEGER, book_no TEXT, title TEXT, author TEXT,
    PRIMARY KEY(year, month, ranking)
)
'''
SQL_CONTAIN_BOOK = '''
SELECT 1 FROM books WHERE book_no=?
'''
SQL_INSERT_ARTICLE = '''
INSERT OR IGNORE INTO articles
    (book_no, isbn, author, publisher, pub_date, title, article) VALUES (?, ?, ?, ?, ?, ?, ?)
'''
SQL_INSERT_BOOK = '''
INSERT OR IGNORE INTO books (book_no, page_cnt) VALUES (?, ?)
'''
SQL_INSERT_RANKING = '''
INSERT OR IGNORE INTO rankings (year, month, ranking, book_no, title, author) VALUES (?, ?, ?, ?, ?, ?)
'''

URL_MONTHTOPB = 'http://www.books.com.tw/web/sys_monthtopb/books/?year={0}&month={1}'
URL_PRODUCT = 'http://www.books.com.tw/products/{0}'
URL_SERIALTEXT = 'http://www.books.com.tw/web/sys_serialtext/?item={0}'
URL_SERIALTEXT_PAGE = URL_SERIALTEXT + '&page={1}'

# num_char:   number of character in raw_text except space and new-line
# num_hanzi:  number of hanzi in raw_text
# num_unique: number of unique hanze in raw_text
SQL_CREATE_CORPUS = '''
CREATE TABLE IF NOT EXISTS
    corpus (src TEXT, idx TEXT, raw_text TEXT, stats TEXT, num_char INTEGER, num_hanzi INTEGER, num_unique INTEGER,
    PRIMARY KEY(src, idx))
'''
SQL_INSERT_CORPUS = '''
INSERT OR IGNORE INTO corpus VALUES (?, ?, ?, ?, ?, ?, ?)
'''
SQL_SELECT_BOOKS = '''
SELECT * FROM books WHERE page_count > 0
'''


class SqliteWriter():
    """SQLite Writer
    """

    def __init__(self):
        self.conn = sqlite3.connect('books.db')
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_TABLE_ARTICLES)
        self.conn.commit()
        cur.close()

    def select_book(self):
        """select_book
        """
        cur = self.conn.cursor()
        for row in cur.execute(SQL_SELECT_BOOKS):
            src = 'books'
            idx = row[0]
            raw_text = '{0}\n\n{1}\n{2}'.format(row[1], row[2], row[5])
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
            print('{0} [INFO] Calc book[{1}] ... num(char/hanzi/unique) = {2}/{3}/{4}'.format(
                datetime_iso(), row[0], num_char, num_hanzi, num_unique))
            # cur_ins.close()
        cur.close()
        # self.corpus.commit()

    def book_no_list(self):
        """insert_published
        """
        cur = self.conn.cursor()
        result = [(row[0], row[1]) for row in cur.execute(
            'SELECT book_no, published FROM books')]
        cur.close()
        return result

    def update_published(self, book_no, published):
        """update_published
        """
        cur = self.conn.cursor()
        cur.execute('UPDATE books SET published=? WHERE book_no=?',
                    (published, book_no))
        self.conn.commit()
        cur.close()


class BooksCrawler():
    """Crawler of Books.com
    """

    def __init__(self, writer):
        self.writer = writer
        self.urlopen_count = 0
        self.init_db()
        self.init_logger()
        self.logger.info('---- [BooksCrawler] ------------------------------')

    def init_db(self):
        """init db
        """
        self.conn = sqlite3.connect('source-books.db')
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_TABLE_ARTICLES)
        cur.execute(SQL_CREATE_TABLE_BOOKS)
        cur.execute(SQL_CREATE_TABLE_RANKINGS)
        self.conn.commit()
        cur.close()

    def init_logger(self):
        """init logger
        """
        self.logger = logging.getLogger('books')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        ifh = logging.FileHandler('crawler-books.log', encoding='utf-8')
        ifh.setFormatter(formatter)
        ifh.setLevel(logging.INFO)
        wfh = logging.FileHandler('crawler-books-warn.log', encoding='utf-8')
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
        if self.urlopen_count % 100 == 0:
            for _ in range(0, randint(24, 96), 2):
                print('=', end='', flush=True)
                sleep(2)
        elif self.urlopen_count % 10 == 0:
            for _ in range(randint(12, 20)):
                print('-', end='', flush=True)
                sleep(1)
        else:
            sleep(randint(3, 5))

    def contain_book(self, book_no):
        """check if contains book
        """
        cur = self.conn.cursor()
        cur.execute(SQL_CONTAIN_BOOK, [book_no])
        result = True if cur.fetchone() is not None else False
        cur.close()
        return result

    def get_page_count(self, book_no):
        """get page count
        """
        self.sleep()
        url = URL_SERIALTEXT.format(book_no)
        try:
            soup = BeautifulSoup(urlopen(url), PARSER)
            span = soup.find_all('div', {'class': 'page'})[-1].span
            page_cnt = int(span.text)
            soup.decompose()
            return page_cnt
        except HTTPError:
            return 0

    def get_book_info(self, book_no):
        """get book info
        """
        self.sleep()
        url = URL_PRODUCT.format(book_no)
        try:
            soup = BeautifulSoup(urlopen(url), PARSER)
            list_item = soup.find('li', {'itemprop': 'author'})
            while list_item is not None:
                li_text = list_item.text.strip()
                if u'出版社' in li_text:
                    publisher = list_item.find('a').text
                elif u'出版日期' in li_text:
                    pub_date = li_text[5:].replace('/', '-')
                list_item = list_item.find_next_sibling('li')
            meta = soup.find('meta', {'itemprop': 'productID'})
            isbn = meta['content'][5:]
            return [isbn, publisher, pub_date]
        except HTTPError:
            return []

    def insert_book(self, book_no, page_cnt):
        """insert book
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_BOOK, (book_no, page_cnt))
        self.conn.commit()
        cur.close()

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

    def crawl_book(self, book_no, title, author):
        """crawl book
        return values list of book
        """
        self.logger.info('fetching book[%s] %s...', book_no, title)

        if self.contain_book(book_no):
            self.logger.info('      -> book[%s] contained and skip', book_no)
            return
        page_cnt = self.get_page_count(book_no)
        if page_cnt <= 0:
            self.logger.info('      -> book[%s] has no preview', book_no)
            self.insert_book(book_no, page_cnt)
            return
        book_info = self.get_book_info(book_no)
        if len(book_info) == 0:
            self.logger.warning('      -> book[%s] cannot get info', book_no)
            book_info = ['', '', '1970-01-01']

        cont = ''
        for i in range(1, page_cnt + 1):
            self.sleep()
            url = URL_SERIALTEXT_PAGE.format(book_no, i)
            soup = BeautifulSoup(urlopen(url), PARSER)
            text = soup.find_all('div', {'class': 'cont'})[-1].text
            cont += text
            soup.decompose()

        article_values = [book_no, book_info[0], author,
                          book_info[1], book_info[2], title, cont]
        self.insert_book(book_no, page_cnt)
        self.insert_article(article_values)
        self.logger.info(
            '      -> book[%s] %s %d pages saved', book_no, title, page_cnt)

    def crawl_month(self, year, month):
        """crawl_month
        """
        self.logger.info('fetching TOP 100 of %4d-%02d...', year, month)
        self.sleep()
        url = URL_MONTHTOPB.format(year, month)
        soup = BeautifulSoup(urlopen(url), PARSER)
        for div in soup.find_all('div', {'class': 'type02_bd-a'}):
            top_no = div.parent.find('strong', {'class': 'no'}).text
            title = div.h4.text
            author = div.ul.li.text[3:]
            href = div.h4.a.get('href')
            book_no = href.rsplit('/', 1)[-1].split('?')[0]
            self.insert_ranking([year, month, top_no, book_no, title, author])
            self.crawl_book(book_no, title, author)
        soup.decompose()

    def fetch_all(self):
        """fetch_all
        """
        start = (2013, 7)
        end = (2017, 6)
        for year in range(start[0], end[0] + 1):
            start_month = start[1] if year == start[0] else 1
            end_month = end[1] if year == end[0] else 13
            for month in range(start_month, end_month):
                self.crawl_month(year, month)

    def calc_all(self):
        """calc_all
        """
        t_start = time()
        self.writer.select_book()
        t_end = time()
        print(t_end - t_start)


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
        CRAWLER = BooksCrawler(WRITER)
        CRAWLER.fetch_all()
    elif sys.argv[1] == 'calc':
        WRITER = SqliteWriter()
        CRAWLER = BooksCrawler(WRITER)
        CRAWLER.calc_all()
    elif sys.argv[1] == 'test':
        WRITER = SqliteWriter()
        CRAWLER = BooksCrawler(WRITER)
        # CRAWLER.crawl_book('0010743217', '房思琪的初戀樂園', '林奕含')
        CRAWLER.crawl_month(2017, 5)
