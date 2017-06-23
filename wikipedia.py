#!/usr/bin/env python3
"""Crawler of Wikipedia
"""

import json
import logging
import sqlite3
import sys
from time import sleep
from urllib.request import urlopen
from utils import datetime_iso, is_unihan
from bs4 import BeautifulSoup

PARSER = 'html.parser'

WIKI_LIST = 'https://zh.wikipedia.org/zh-tw/Wikipedia:%E7%89%B9%E8%89%B2%E6%9D%A1%E7%9B%AE'
WIKI_ARTICLE = 'https://zh.wikipedia.org/zh-tw/{0}'

SQL_CREATE_ARTICLES = '''
CREATE TABLE IF NOT EXISTS
    articles (title TEXT, category TEXT, href TEXT, cont TEXT,
    PRIMARY KEY(title))
'''
# num_char:   number of character in raw_text except space and new-line
# num_hanzi:  number of hanzi in raw_text
# num_unique: number of unique hanze in raw_text
SQL_CREATE_CORPUS = '''
CREATE TABLE IF NOT EXISTS
    corpus (src TEXT, idx TEXT, raw_text TEXT, stats TEXT, num_char INTEGER, num_hanzi INTEGER, num_unique INTEGER,
    PRIMARY KEY(src, idx))
'''
SQL_INSERT_ARTICLES = '''
INSERT OR IGNORE INTO articles VALUES (?, ?, ?, ?)
'''
SQL_INSERT_CORPUS = '''
INSERT OR IGNORE INTO corpus VALUES (?, ?, ?, ?, ?, ?, ?)
'''
SQL_EXISTS_ARTICLES= '''
SELECT 1 FROM articles WHERE title=?
'''
SQL_SELECT_ARTICLES = '''
SELECT title, cont FROM articles
'''


class SqliteWriter():
    """SQLite Writer
    """

    def __init__(self):
        self.conn = sqlite3.connect('wikipedia.db')
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_ARTICLES)
        self.conn.commit()
        cur.close()

        self.corpus = sqlite3.connect('corpus.db')
        cur = self.corpus.cursor()
        cur.execute(SQL_CREATE_CORPUS)
        self.corpus.commit()
        cur.close()

    def write_article(self, title, href, cate, cont):
        """write_article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_ARTICLES, (title, cate, href, cont))
        self.conn.commit()
        cur.close()

    def contains_article(self, title):
        """contains_article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_EXISTS_ARTICLES, [title])
        self.conn.commit()
        result = True if cur.fetchone() is not None else False
        cur.close()
        return result

    def select_articles(self):
        """select_articles
        """
        cur = self.conn.cursor()
        for row in cur.execute(SQL_SELECT_ARTICLES):
            src = 'wikipedia'
            idx = row[0]
            raw_text = '{0}\n\n{1}'.format(row[0], row[1])
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
            stats = json.dumps(char_freq_table, ensure_ascii=False, sort_keys=True).encode('utf-8')
            cur_ins = self.corpus.cursor()
            cur_ins.execute(SQL_INSERT_CORPUS, (src, idx, raw_text,
                                                stats, num_char, num_hanzi, num_unique))
            print('{0} [INFO] Calc articles[{1}] ... num(char/hanzi/unique) = {2}/{3}/{4}'.format(
                datetime_iso(), row[0], num_char, num_hanzi, num_unique))
            cur_ins.close()
        cur.close()
        self.corpus.commit()


class Wikipedia():
    """Crawler of Wikipedia
    """

    def __init__(self, writer):
        self.writer = writer
        self.urlopen_count = 0
        logging.basicConfig(
            format='%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    def sleep(self):
        """sleep
        """
        sleep(1)

    def fetch_article(self, idx, title, href, cate):
        """fetch_article
        """
        print('{0} [INFO] {3:03} Fetching Article: [{1}]({2})'.format(
            datetime_iso(), title, href, idx), end='', flush=True)
        if self.writer.contains_article(title):
            print(' -> contained and skip')
            return
        print('.', end='', flush=True)
        self.sleep()
        url = WIKI_ARTICLE.format(href[6:])
        soup = BeautifulSoup(urlopen(url), PARSER)

        # remove siteSub
        soup.find(id='siteSub').extract()
        print('.', end='', flush=True)
        # remove TOC
        if soup.find(id='toc' is not None):
            soup.find(id='toc').extract()
        print('.', end='', flush=True)
        # remove div class=refbegin (reference)
        for elem in soup.find_all('div', {'class': 'refbegin'}):
            elem.extract()
        print('.', end='', flush=True)
        # remove div class=reflist (reference)
        for elem in soup.find_all('div', {'class': 'reflist'}):
            elem.extract()
        print('.', end='', flush=True)
        # remove all edit tag
        for elem in soup.find_all('span', {'class': 'mw-editsection'}):
            elem.extract()
        print('.', end='', flush=True)
        # remove table infoBox
        for elem in soup.find_all('table', {'class': 'infobox'}):
            elem.extract()
        print('.', end='', flush=True)
        # remove table navbox
        for elem in soup.find_all('table', {'class': 'navbox'}):
            elem.extract()
        print('.', end='', flush=True)
        # remove table succession-box
        for elem in soup.find_all('table', {'class': 'succession-box'}):
            elem.extract()
        print('.', end='', flush=True)
        # remove class=noprint
        for elem in soup.find_all(attrs={'class': 'noprint'}):
            elem.extract()
        print('.', end='', flush=True)

        cont = soup.find(id='mw-content-text')
        if cont is None:
            print(' -> ERROR: no content')
            return

        soup.decompose()
        self.writer.write_article(title, href, cate, cont.text)
        print('{0} char saved'.format(len(cont.text)))
        return

    def fetch_all(self):
        """fetch_all
        """
        soup = BeautifulSoup(urlopen(WIKI_LIST), PARSER)

        # starting tag node of featured article links
        node = soup.find(id='content').find_all('table')[3].find_all('td')[0]

        # parse page content and generate category list
        articles = []
        cate_h2 = None
        cate_h3 = None
        cate_h4 = None
        curr_level = 2
        for child in node.children:
            if child.name is None:
                continue
            elif child.name == 'h2':
                cate_h2 = child.text
                cate_h3 = None
                cate_h4 = None
                curr_level = 2
            elif child.name == 'h3':
                cate_h3 = child.text
                cate_h4 = None
                curr_level = 3
            elif child.name == 'h4':
                cate_h4 = child.text
                curr_level = 4
            elif child.name == 'p':
                if curr_level == 2:
                    category = '{0}'.format(cate_h2)
                elif curr_level == 3:
                    category = '{0}/{1}'.format(cate_h2, cate_h3)
                elif curr_level == 4:
                    category = '{0}/{1}/{2}'.format(cate_h2, cate_h3, cate_h4)
                for tag in child.find_all('a'):
                    if 'File:' in tag.get('href'):
                        continue
                    articles.append(
                        [tag.get_text(), tag.get('href'), category])
            elif child.name == 'ul':
                break
            else:
                print('[Error] unexpected tag: <{0}>'.format(child.name))
                break

        soup.decompose()

        # save to db
        for idx, art in enumerate(articles):
            self.fetch_article(idx, art[0], art[1], art[2])
            # print('{2}/{0}({1})'.format(*art))

    def calc_all(self):
        """calc_all
        """
        self.writer.select_articles()

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
        WIKI = Wikipedia(WRITER)
        WIKI.fetch_all()
        # print(WIKI.fetch_article('天津市耀華中學', '/wiki/%E5%A4%A9%E6%B4%A5%E5%B8%82%E8%80%80%E5%8D%8E%E4%B8%AD%E5%AD%A6'))
    elif sys.argv[1] == 'calc':
        WRITER = SqliteWriter()
        WIKI = Wikipedia(WRITER)
        WIKI.calc_all()
