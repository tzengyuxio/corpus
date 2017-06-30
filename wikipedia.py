#!/usr/bin/env python3
"""Crawler of Wikipedia
"""

import logging
import sqlite3
import sys
from datetime import datetime
from time import sleep
from urllib.request import urlopen

from bs4 import BeautifulSoup

from utils import PARSER

SQL_CREATE_TABLE_ARTICLES = '''
CREATE TABLE IF NOT EXISTS articles (
    title TEXT, open_date TEXT, category TEXT, url TEXT, article TEXT,
    PRIMARY KEY(title, open_date))
'''
SQL_CONTAIN_ARTICLE = '''
SELECT 1 FROM articles WHERE title=?
'''
SQL_INSERT_ARTICLE = '''
INSERT OR IGNORE INTO articles (title, open_date, category, url, article) VALUES (?, ?, ?, ?, ?)
'''

URL_WIKI_LIST = 'https://zh.wikipedia.org/zh-tw/Wikipedia:%E7%89%B9%E8%89%B2%E6%9D%A1%E7%9B%AE'
URL_WIKI_ARTICLE = 'https://zh.wikipedia.org/zh-tw/{0}'


class WikipediaCrawler():
    """Crawler of Wikipedia
    """

    def __init__(self):
        self.urlopen_count = 0
        self.init_db()
        self.init_logger()
        self.logger.info('---- [WikipediaCrawler] ---------------------------')

    def init_db(self):
        """init db
        """
        self.conn = sqlite3.connect('source-wikipedia.db')
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_TABLE_ARTICLES)
        self.conn.commit()
        cur.close()

    def init_logger(self):
        """init logger
        """
        self.logger = logging.getLogger('magcnyes')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-7s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        ifh = logging.FileHandler('crawler-wikipedia.log', encoding='utf-8')
        ifh.setFormatter(formatter)
        ifh.setLevel(logging.INFO)
        wfh = logging.FileHandler(
            'crawler-wikipedia-warn.log', encoding='utf-8')
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
        sleep(1)

    def contain_article(self, title):
        """check if contains article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_CONTAIN_ARTICLE, [title])
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

    def fetch_article(self, idx, title, href, cate):
        """fetch_article
        """
        self.logger.info('%03d fetching article [%s](%s)...', idx, title, href)
        if self.contain_article(title):
            self.logger.info(
                '          -> article [%s] contained and skip', title)
            return
        self.sleep()
        url = URL_WIKI_ARTICLE.format(href[6:])
        soup = BeautifulSoup(urlopen(url), PARSER)

        # remove siteSub
        soup.find(id='siteSub').extract()
        # remove TOC
        if soup.find(id='toc' is not None):
            soup.find(id='toc').extract()
        # remove div class=refbegin (reference)
        for elem in soup.find_all('div', {'class': 'refbegin'}):
            elem.extract()
        # remove div class=reflist (reference)
        for elem in soup.find_all('div', {'class': 'reflist'}):
            elem.extract()
        # remove all edit tag
        for elem in soup.find_all('span', {'class': 'mw-editsection'}):
            elem.extract()
        # remove table infoBox
        for elem in soup.find_all('table', {'class': 'infobox'}):
            elem.extract()
        # remove table navbox
        for elem in soup.find_all('table', {'class': 'navbox'}):
            elem.extract()
        # remove table succession-box
        for elem in soup.find_all('table', {'class': 'succession-box'}):
            elem.extract()
        # remove class=noprint
        for elem in soup.find_all(attrs={'class': 'noprint'}):
            elem.extract()

        cont = soup.find(id='mw-content-text')
        if cont is None:
            self.logger.error(
                '          -> article [%s] has no content', title)
            return

        soup.decompose()
        the_date = datetime.now().strftime('%Y-%m-%d')
        art_val = [title, the_date, cate, href, cont.text.strip()]
        self.insert_article(art_val)
        self.logger.info(
            '          -> article [%s] with %d char saved', title, len(cont.text))
        return

    def fetch_all(self):
        """fetch_all
        """
        soup = BeautifulSoup(urlopen(URL_WIKI_LIST), PARSER)

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
                self.logger.error('unexpected tag: <%s>', child.name)
                break

        soup.decompose()

        # save to db
        for idx, art in enumerate(articles):
            self.fetch_article(idx, art[0], art[1], art[2])
            # print('{2}/{0}({1})'.format(*art))


def print_usage():
    """Print Usage
    """
    print('usage: {0} command'.format(sys.argv[0]))
    print('')
    print('    all     fetch all')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(0)
    elif sys.argv[1] == 'all':
        WIKI = WikipediaCrawler()
        WIKI.fetch_all()
        # print(WIKI.fetch_article('天津市耀華中學', '/wiki/%E5%A4%A9%E6%B4%A5%E5%B8%82%E8%80%80%E5%8D%8E%E4%B8%AD%E5%AD%A6'))
    elif sys.argv[1] == 'calc':
        WIKI = WikipediaCrawler()
        WIKI.calc_all()
