#!/usr/bin/env python3
"""Crawler of Books.com
"""
import logging
import sqlite3
import sys
from random import randint
from time import sleep
from urllib.error import HTTPError
from urllib.request import urlopen

from bs4 import BeautifulSoup

from utils import PARSER, is_unihan

import time
from functools import wraps
import requests
from requests.exceptions import ConnectionError, Timeout

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
SQL_CREATE_TABLE_SUBJECTS = '''
CREATE TABLE IF NOT EXISTS subjects (
    subj TEXT, ranking INTEGER, book_no TEXT, title TEXT, author TEXT,
    PRIMARY KEY(subj, ranking)
)
'''
SQL_CREATE_TABLE_DONE_SUBJECTS = '''
CREATE TABLE IF NOT EXISTS done_subjects (
    subj_no TEXT, name TEXT, full_name TEXT,
    PRIMARY KEY(subj_no)
)
'''
SQL_CONTAIN_BOOK = '''
SELECT 1 FROM books WHERE book_no=?
'''
SQL_CONTAIN_DONE_SUBJECT = '''
SELECT 1 FROM done_subjects WHERE subj_no=?
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
SQL_INSERT_SUBJECT = '''
INSERT OR IGNORE INTO subjects (subj, ranking, book_no, title, author) VALUES (?, ?, ?, ?, ?)
'''
SQL_INSERT_DONE_SUBJECT = '''
INSERT OR IGNORE INTO done_subjects (subj_no, name, full_name) VALUES (?, ?, ?)
'''

URL_MONTHTOPB = 'http://www.books.com.tw/web/sys_monthtopb/books/?year={0}&month={1}'
URL_PRODUCT = 'http://www.books.com.tw/products/{0}'
URL_SERIALTEXT = 'http://www.books.com.tw/web/sys_serialtext/?item={0}'
URL_SERIALTEXT_PAGE = URL_SERIALTEXT + '&page={1}'
URL_SUBLISTB = 'https://www.books.com.tw/web/sys_sublistb/books/?loc=subject_011'


def print_cate_tree(cate, tier):
    """print_cate_tree
    有 950 個分類要查
    """
    print(' ' * tier * 4 + cate[0])
    if len(cate[2]) != 0:
        for subcate in cate[2]:
            print_cate_tree(subcate, tier+1)

def retry(exceptions, tries=4, delay=3, backoff=2, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = '{}, Retrying in {} seconds...'.format(e, mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


@retry(requests.exceptions.Timeout, delay=60)
def call_books(url):
    return requests.get(url, headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'})


class BooksCrawler():
    """Crawler of Books.com
    """

    def __init__(self):
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
        cur.execute(SQL_CREATE_TABLE_SUBJECTS)
        cur.execute(SQL_CREATE_TABLE_DONE_SUBJECTS)
        self.conn.commit()
        cur.close()

    def init_logger(self):
        """init logger
        """
        self.logger = logging.getLogger('books')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-7s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

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
            seconds = randint(24, 96)
            print('(count: {0}, sleep {1} seconds) ['.format(
                self.urlopen_count, seconds), end='')
            for _ in range(0, seconds, 2):
                print(':', end='', flush=True)
                sleep(2)
            print(']')
        elif self.urlopen_count % 10 == 0:
            seconds = randint(12, 20)
            print('(count: {0}, sleep {1} seconds) ['.format(
                self.urlopen_count, seconds), end='')
            for _ in range(seconds):
                print('.', end='', flush=True)
                sleep(1)
            print(']')
        else:
            sleep(randint(3, 7))

    def contain_book(self, book_no):
        """check if contains book
        """
        cur = self.conn.cursor()
        cur.execute(SQL_CONTAIN_BOOK, [book_no])
        result = True if cur.fetchone() is not None else False
        cur.close()
        return result

    def contain_done_subject(self, subj_no):
        """check if contains done subject
        """
        cur = self.conn.cursor()
        cur.execute(SQL_CONTAIN_DONE_SUBJECT, [subj_no])
        result = True if cur.fetchone() is not None else False
        cur.close()
        return result

    def get_page_count(self, book_no):
        """get page count
        """
        self.sleep()
        url = URL_SERIALTEXT.format(book_no)
        try:
            req = call_books(url)
            soup = BeautifulSoup(req.text, PARSER)
            span = soup.find_all('div', {'class': 'page'})[-1].span
            page_cnt = int(span.text)
            soup.decompose()
            return page_cnt
        except IndexError:
            # print('--IndexError----------------------------')
            # print(url)
            # print('----------------------------------------')
            # print(req.text)
            # quit()
            return 0
        except HTTPError:
            return 0

    def get_book_info(self, book_no):
        """get book info
        """
        self.sleep()
        url = URL_PRODUCT.format(book_no)
        try:
            req = call_books(url)
            soup = BeautifulSoup(req.text, PARSER)
            meta = soup.find('meta', {'itemprop': 'productID'})
            if meta is None:
                return []
            isbn = meta['content'][5:]

            list_item = soup.find('li', {'itemprop': 'author'})
            while list_item is not None:
                li_text = list_item.text.strip()
                if u'出版社' in li_text:
                    publisher = list_item.find('a').text
                elif u'出版日期' in li_text:
                    pub_date = li_text[5:].replace('/', '-')
                list_item = list_item.find_next_sibling('li')
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

    def insert_subject(self, subject_values):
        """insert subject
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_SUBJECT, subject_values)
        self.conn.commit()
        cur.close()

    def insert_done_subject(self, subj_no, name, full_name):
        """insert done subject
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_DONE_SUBJECT, (subj_no, name, full_name))
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
            req = call_books(url)
            soup = BeautifulSoup(req.text, PARSER)
            conts = soup.find_all('div', {'class': 'cont'})
            while len(conts) != 0:
                self.logger.warning('      -> book[%s][%d] IndexError, retry...%s', book_no, i, url)
                sleep(36)
                req = call_books(url)
                soup = BeautifulSoup(req.text, PARSER)
                conts = soup.find_all('div', {'class': 'cont'})
            # text = soup.find_all('div', {'class': 'cont'})[-1].text
            text = conts[-1].text
            cont += text
            soup.decompose()

        article_values = [book_no, book_info[0], author,
                          book_info[1], book_info[2], title, cont]
        self.insert_book(book_no, page_cnt)
        self.insert_article(article_values)
        self.logger.info(
            '      -> book[%s] %s %d pages saved', book_no, title, page_cnt)
        sleep(24)

    def crawl_month(self, year, month):
        """crawl_month
        爬榜，會將每個月排行榜資料放到 `rankings` TABLE
        對排行榜上每本書做資料收集
        """
        self.logger.info('fetching TOP 100 of %4d-%02d...', year, month)
        self.sleep()
        url = URL_MONTHTOPB.format(year, month)
        req = call_books(url)
        soup = BeautifulSoup(req.text, PARSER)
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
        2013 七月 ~ 2017 七月 共 49 個月的排行榜資料
        對每個月做月排行榜資料收集
        """
        start = (2013, 7)
        end = (2017, 7)
        for year in range(start[0], end[0] + 1):
            start_month = start[1] if year == start[0] else 1
            end_month = end[1] if year == end[0] else 13
            for month in range(start_month, end_month):
                self.crawl_month(year, month)

    def fetch_one_category(self, cate, parents):
        """fetch_one_category
        """
        subj_no = cate[1].rsplit('=', 1)[-1]
        cate_name = parents + ('>' + cate[0])
        if len(cate[2]) != 0:
            # parents = cate[1] if parents == '' else parents + '/' + cate[1]
            for sub_cate in cate[2]:
                self.fetch_one_category(sub_cate, cate_name)
        else:
            if self.contain_done_subject(subj_no):
                self.logger.info('      -> subject[%s] contained and skip', cate[0])
                return
            self.logger.info('fetching TOP 100 of %s...', cate_name)
            self.sleep()
            tail = cate[1].rsplit('/', 1)[-1]
            url = cate[1].replace(tail, '?v=1&o=5')
            req = call_books(url)
            soup = BeautifulSoup(req.text, PARSER)
            top_no = 0
            for h4 in soup.find_all('h4'):
                div_text_cont = h4.find_next_sibling('div')
                if div_text_cont is None:
                    continue
                top_no += 1
                # top_no = div.parent.find('strong', {'class': 'no'}).text
                title = h4.text
                author = h4.find_next_sibling('ul').li.a.text
                href = h4.a.get('href')
                book_no = href.rsplit('/', 1)[-1].split('?')[0]
                self.insert_subject([cate_name, top_no, book_no, title, author])
                self.crawl_book(book_no, title, author)
                # print(book_no, title, author, href)
            soup.decompose()
            self.insert_done_subject(subj_no, cate[0], cate_name)

    def fetch_all_categories(self):
        """fetch_all_categories
        """
        req = call_books(URL_SUBLISTB)
        soup = BeautifulSoup(req.text, PARSER)
        cate0 = ('中文書', URL_SUBLISTB, [])
        for h4 in soup.find_all('h4'):
            tbl = h4.find_next_sibling('table')
            if tbl is None:
                continue
            cate1 = (h4.text, h4.a.get('href'), [])
            cate0[2].append(cate1)
            for tr in tbl.find_all('tr'):
                cate2 = (tr.th.h5.text, tr.th.h5.a.get('href'), [])
                cate1[2].append(cate2)
                for li in tr.td.find_all('li'):
                    if li.a is None:
                        continue
                    cate3 = (li.text, li.a.get('href'), [])
                    cate2[2].append(cate3)
        soup.decompose()
        # print_cate_tree(cate0, 0)
        self.fetch_one_category(cate0, '')


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
        CRAWLER = BooksCrawler()
        CRAWLER.fetch_all()
    elif sys.argv[1] == 'allcates':
        CRAWLER = BooksCrawler()
        CRAWLER.fetch_all_categories()
    elif sys.argv[1] == 'test':
        CRAWLER = BooksCrawler()
        # CRAWLER.crawl_book('0010743217', '房思琪的初戀樂園', '林奕含')
        # CRAWLER.crawl_month(2017, 5)
        # CRAWLER.crawl_book('0010592444', '謎情柯洛斯III', '林奕含')
        CRAWLER.crawl_book('0010521950', '怦然心動的人生整理魔法', '近藤麻理惠')

# TODO
# 1. request timeout 408
# 2. dont fetch page again
