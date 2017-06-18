#!/usr/bin/env python3
"""Crawler of Books.com
"""

import logging
import sqlite3
import sys
from datetime import datetime
from random import randint
from time import sleep
from urllib.error import HTTPError
from urllib.request import urlopen

from bs4 import BeautifulSoup

PARSER = 'html.parser'

MONTHTOPB = 'http://www.books.com.tw/web/sys_monthtopb/books/?year={0}&month={1}'
PRODUCT = 'http://www.books.com.tw/products/{0}'
SERIALTEXT = 'http://www.books.com.tw/web/sys_serialtext/?item={0}'
SERIALTEXT_PAGE = SERIALTEXT + '&page={1}'

SQL_CREATE_BOOK_TOPS = '''
CREATE TABLE IF NOT EXISTS
    book_tops (year INTEGER, month INTEGER, top INTEGER, book_no TEXT, title TEXT, author TEXT,
    PRIMARY KEY(year, month, top))
'''
SQL_CREATE_BOOKS = '''
CREATE TABLE IF NOT EXISTS
    books (book_no TEXT, title TEXT, author TEXT, page_count INTEGER, cont TEXT,
    PRIMARY KEY(book_no))
'''
# num_char:   number of character in raw_text except space and new-line
# num_hanzi:  number of hanzi in raw_text
# num_unique: number of unique hanze in raw_text
SQL_CREATE_CORPUS = '''
CREATE TABLE IF NOT EXISTS
    corpus (src TEXT, idx TEXT, raw_text TEXT, stats TEXT, num_char INTEGER, num_hanzi INTEGER, num_unique INTEGER,
    PRIMARY KEY(src, idx))
'''
SQL_INSERT_BOOK_TOPS = '''
INSERT OR IGNORE INTO book_tops VALUES (?, ?, ?, ?, ?, ?)
'''
SQL_INSERT_BOOKS = '''
INSERT OR IGNORE INTO books VALUES (?, ?, ?, ?, ?)
'''
SQL_EXISTS_BOOKS = '''
SELECT 1 FROM books WHERE book_no=?
'''
SQL_SELECT_BOOKS = '''
SELECT * FROM books WHERE page_count > 0
'''


def datetime_iso():
    """datetime_iso
    """
    return datetime.now().replace(microsecond=0).isoformat(' ')


class SqliteWriter():
    """SQLite Writer
    """

    def __init__(self):
        self.conn = sqlite3.connect('corpus.db')
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_BOOK_TOPS)
        cur.execute(SQL_CREATE_BOOKS)
        cur.execute(SQL_CREATE_CORPUS)
        self.conn.commit()
        cur.close()

    def write_top(self, year, month, top_no, book_no, title, author):
        """write_ranking
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_BOOK_TOPS, (year, month,
                                           top_no, book_no, title, author))
        self.conn.commit()
        cur.close()

    def write_book(self, book_no, title, author, page_count, cont):
        """write_book
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_BOOKS,
                    (book_no, title, author, page_count, cont))
        self.conn.commit()
        cur.close()

    def contains_book(self, book_no):
        """contains_book
        """
        cur = self.conn.cursor()
        cur.execute(SQL_EXISTS_BOOKS, [book_no])
        self.conn.commit()
        result = True if cur.fetchone() is not None else False
        cur.close()
        return result

    def select_book(self):
        """select_book
        """
        cur = self.conn.cursor()
        for row in cur.execute(SQL_SELECT_BOOKS):
            print('{0} [INFO] Calc book[{1}]'.format(datetime_iso(), row[0]))
        cur.close()


class Books():
    """Crawler of Books.com
    """

    def __init__(self, writer):
        self.writer = writer
        self.urlopen_count = 0
        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

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

    def test_book(self, book_no):
        """test_book
        """
        self.sleep()
        url = SERIALTEXT.format(book_no)
        try:
            soup = BeautifulSoup(urlopen(url), PARSER)
            page_count = int(soup.find_all(
                'div', {'class': 'page'})[-1].span.text)
            soup.decompose()
            return page_count
        except HTTPError:
            return 0

    def fetch_book(self, book_no, title, author):
        """fetch_book
        """
        print('{0} [INFO]   Fetching Book[{1}] {2}'.format(
            datetime_iso(), book_no, title), end='', flush=True)
        if self.writer.contains_book(book_no):
            print(' -> contained and skip')
            return
        page_count = self.test_book(book_no)
        if page_count <= 0:
            self.writer.write_book(book_no, title, author, page_count, '')
            print(' -> no preview')
            return
        # text = '{0}\n\n{1}\n'.format(title, author)
        text = ''
        for i in range(1, page_count + 1):
            print('.', end='', flush=True)
            self.sleep()
            url = SERIALTEXT_PAGE.format(book_no, i)
            soup = BeautifulSoup(urlopen(url), PARSER)
            cont = soup.find_all('div', {'class': 'cont'})[-1].text
            text += cont
            soup.decompose()
        self.writer.write_book(book_no, title, author, page_count, text)
        print('saved')
        return

    def fetch_month(self, year, month):
        """fetch_mont
        """
        print('{0} [INFO] Processing Top100 of {1}/{2:00}'.format(
            datetime_iso(), year, month))
        self.sleep()
        url = MONTHTOPB.format(year, month)
        soup = BeautifulSoup(urlopen(url), PARSER)
        for div in soup.find_all('div', {'class': 'type02_bd-a'}):
            top_no = div.parent.find_all('strong', {'class': 'no'})[0].text
            title = div.h4.text
            author = div.ul.li.text[3:]
            href = div.h4.a.get('href')
            book_no = href.rsplit('/', 1)[-1].split('?')[0]
            self.writer.write_top(year, month, top_no, book_no, title, author)
            self.fetch_book(book_no, title, author)
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
                self.fetch_month(year, month)

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
        BOOK = Books(WRITER)
        BOOK.fetch_all()
        # BOOK.fetch_month(2017, 4)
        # print(BOOK.test_book('0010723234'))
        # print(BOOK.fetch_book('0010723234', '為了活下去：脫北女孩朴研美', '朴研美', 5))
        # BOOK.fetch_book('0010723234', '為了活下去：脫北女孩朴研美', '朴研美')
    elif sys.argv[1] == 'calc':
        WRITER = SqliteWriter()
        BOOK = Books(WRITER)
        BOOK.calc_all()
