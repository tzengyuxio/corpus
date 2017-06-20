#!/usr/bin/env python3
"""Crawler of Books.com
"""

import json
import logging
import sqlite3
import sys
from random import randint
from time import sleep
from urllib.error import HTTPError
from urllib.request import urlopen

from bs4 import BeautifulSoup

from utils import datetime_iso, is_unihan

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
    books (book_no TEXT, title TEXT, author TEXT, page_count INTEGER, published TEXT, cont TEXT,
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
INSERT OR IGNORE INTO books VALUES (?, ?, ?, ?, ?, ?)
'''
SQL_INSERT_CORPUS = '''
INSERT OR IGNORE INTO corpus VALUES (?, ?, ?, ?, ?, ?, ?)
'''
SQL_EXISTS_BOOKS = '''
SELECT 1 FROM books WHERE book_no=?
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
        cur.execute(SQL_CREATE_BOOK_TOPS)
        cur.execute(SQL_CREATE_BOOKS)
        self.conn.commit()
        cur.close()

        self.corpus = sqlite3.connect('corpus.db')
        cur = self.corpus.cursor()
        cur.execute(SQL_CREATE_CORPUS)
        self.corpus.commit()
        cur.close()

    def write_top(self, year, month, top_no, book_no, title, author):
        """write_ranking
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_BOOK_TOPS, (year, month,
                                           top_no, book_no, title, author))
        self.conn.commit()
        cur.close()

    def write_book(self, book_no, title, author, page_count, published, cont):
        """write_book
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_BOOKS,
                    (book_no, title, author, page_count, published, cont))
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
            cur_ins = self.corpus.cursor()
            cur_ins.execute(SQL_INSERT_CORPUS, (src, idx, raw_text,
                                                stats, num_char, num_hanzi, num_unique))
            print('{0} [INFO] Calc book[{1}] ... num(char/hanzi/unique) = {2}/{3}/{4}'.format(
                datetime_iso(), row[0], num_char, num_hanzi, num_unique))
            self.corpus.commit()
            cur_ins.close()
        cur.close()

    def book_no_list(self):
        """insert_published
        """
        cur = self.conn.cursor()
        result = [(row[0], row[1]) for row in cur.execute('SELECT book_no, published FROM books')]
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

    def book_info(self, book_no):
        """book_info
        """
        self.sleep()
        url = PRODUCT.format(book_no)
        soup = BeautifulSoup(urlopen(url), PARSER)
        more = soup.find('p', {'class': 'more'})
        preview = False if more is None else True
        list_item = soup.find('li', {'itemprop': 'author'})
        published = '1970-01-01'
        while list_item is not None:
            if u'出版日期' in list_item.text:
                published = list_item.text[5:].replace('/', '-')
                break
            else:
                list_item = list_item.find_next_sibling('li')
        return preview, published

    def fetch_book(self, book_no, title, author):
        """fetch_book
        """
        print('{0} [INFO]   Fetching Book[{1}] {2}'.format(
            datetime_iso(), book_no, title), end='', flush=True)
        if self.writer.contains_book(book_no):
            print(' -> contained and skip')
            return
        preview, published = self.book_info(book_no)
        if not preview:
            self.writer.write_book(book_no, title, author, 0, published, '')
            print(' -> no preview')
            return
        page_count = self.test_book(book_no)
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
        self.writer.write_book(book_no, title, author,
                               page_count, published, text)
        print('saved')
        return

    def fetch_month(self, year, month):
        """fetch_mont
        """
        print('{0} [INFO] Processing Top100 of {1}-{2:02}'.format(
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

    def insert_published(self):
        """insert_published
        """
        for book_no, published in self.writer.book_no_list():
            print('{0} [INFO] Writing book[{1}]...'.format(
                datetime_iso(), book_no), end='', flush=True)
            if published is not None:
                print('"{0}" -> skipped.'.format(published))
                continue
            _, published = self.book_info(book_no)
            print('with published date: "{0}"'.format(published))
            self.writer.update_published(book_no, published)


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
    elif sys.argv[1] == 'date':
        WRITER = SqliteWriter()
        BOOK = Books(WRITER)
        BOOK.insert_published()
        # CONTAINS, DATE = BOOK.book_info('0010592120')
        # CONTAINS, DATE = BOOK.book_info('0010592301')
        # print(CONTAINS)
        # print(DATE)
