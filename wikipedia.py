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
    title TEXT, open_date TEXT, quality TEXT, category TEXT, url TEXT, article TEXT,
    PRIMARY KEY(title, open_date))
'''
SQL_CONTAIN_ARTICLE = '''
SELECT 1 FROM articles WHERE title=?
'''
SQL_INSERT_ARTICLE = '''
INSERT OR IGNORE INTO articles (title, open_date, quality, category, url, article) VALUES (?, ?, ?, ?, ?, ?)
'''

# URL_WIKI_FA_LIST = 'https://zh.wikipedia.org/zh-tw/Wikipedia:%E7%89%B9%E8%89%B2%E6%9D%A1%E7%9B%AE'
# URL_WIKI_GA_LIST = 'https://zh.wikipedia.org/zh-tw/Wikipedia:%E5%84%AA%E8%89%AF%E6%A2%9D%E7%9B%AE'
URL_WIKI_FA_LIST = 'https://zh.wikipedia.org/zh-tw/Wikipedia:%E5%85%B8%E8%8C%83%E6%9D%A1%E7%9B%AE'
URL_WIKI_GA_LIST = 'https://zh.wikipedia.org/zh-tw/Wikipedia:%E4%BC%98%E8%89%AF%E6%9D%A1%E7%9B%AE'
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

    def fetch_article(self, idx, title, href, cate, quality):
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
        if soup.find(id='toc') is not None:
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
        art_val = [title, the_date, quality, cate, href, cont.text.strip()]
        self.insert_article(art_val)
        self.logger.info(
            '          -> article [%s] with %d char saved', title, len(cont.text))
        return

    def find_cate(self, node):
        h2 = node.find_previous_sibling('h2')
        if h2 is None:
            return self.find_cate(node.parent)
        return h2.text

    def fetch_all_featured(self):
        """fetch_all featured
        """
        soup = BeautifulSoup(urlopen(URL_WIKI_FA_LIST), PARSER)

        # starting tag node of featured article links
        node = soup.find(id='mw-content-text').find_all('table')[3].find_all('td')[0]
        # print(node.text[:100])

        articles = []
        for a_tag in node.find_all('a'):
            a_href = a_tag.get('href')
            if 'index.php' in a_href or 'meta.wikimedia.org' in a_href:
                continue
            articles.append([a_tag.text, a_href, self.find_cate(a_tag)])
        soup.decompose()

        # save to db
        for idx, art in enumerate(articles):
            self.fetch_article(idx + 1, art[0], art[1], art[2], "featured")
            # print('{3} {2}/{0}({1})'.format(*art, idx+1))

    def fetch_all_good(self):
        """fetch_all good
        """
        soup = BeautifulSoup(urlopen(URL_WIKI_GA_LIST), PARSER)

        # starting tag node of featured article links
        node = soup.find(id='content').find(
            'table', {'class': 'prettytable'})  # .find('tbody')

        if node is None:
            print('node is None')

        # parse page content and generate category list
        articles = []
        cate_h2 = None
        cate_h3 = None
        cate_h4 = None
        # curr_level = 2
        for td_tag in node.find_all('td'):
            for child in td_tag.children:
                if child.name is None:
                    continue
                elif child.name == 'h3':
                    cate_h2 = child.text.split('（')[0]
                    cate_h3 = None
                    cate_h4 = None
                    # curr_level = 2
                    # print('{0}: {1}'.format(curr_level, cate_h2))
                elif child.name == 'ul':
                    for li_tag in child.find_all('li'):
                        cate_h3 = li_tag.find('b').text
                        cate_h4 = None
                        # curr_level = 3
                        # print('  {0}: {1}'.format(curr_level, cate_h3))
                        category = '{0}/{1}'.format(cate_h2, cate_h3)
                        for tag in li_tag.find_all('a'):
                            if 'File:' in tag.get('href'):
                                continue
                            articles.append(
                                [tag.get_text(), tag.get('href'), category])
                elif child.name == 'dl':
                    for dd_tag in child.find_all('dd'):
                        cate_h4 = dd_tag.find('b').text
                        # curr_level = 4
                        # print('    {0}: {1}'.format(curr_level, cate_h4))
                        category = '{0}/{1}/{2}'.format(cate_h2,
                                                        cate_h3, cate_h4)
                        for tag in dd_tag.find_all('a'):
                            if 'File:' in tag.get('href'):
                                continue
                            articles.append(
                                [tag.get_text(), tag.get('href'), category])
        soup.decompose()

        # save to db
        for idx, art in enumerate(articles):
            self.fetch_article(idx+1, art[0], art[1], art[2], "good")
            # print('{3} {2}/{0}({1})'.format(*art, idx + 1))


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
        WIKI.fetch_all_featured()
        WIKI.fetch_all_good()
        # print(WIKI.fetch_article('天津市耀華中學', '/wiki/%E5%A4%A9%E6%B4%A5%E5%B8%82%E8%80%80%E5%8D%8E%E4%B8%AD%E5%AD%A6'))
