#!/usr/bin/env python3
"""Crawler of appledaily forum with given author id
"""
import logging
import sqlite3
import sys
from random import randint
from time import sleep
from urllib.request import urljoin, urlopen

from bs4 import BeautifulSoup

from utils import PARSER, date_iso, iri_to_uri

SQL_CREATE_TABLE_ARTICLES = '''
CREATE TABLE IF NOT EXISTS articles (
    art_id TEXT, forum_id, forum_name TEXT, author TEXT, pub_date TEXT, title TEXT, subtitle TEXT, article TEXT,
    PRIMARY KEY(art_id)
)
'''
SQL_CONTAIN_ARTICLE = '''
SELECT 1 FROM articles WHERE art_id=?
'''
SQL_INSERT_ARTICLE = '''
INSERT OR IGNORE INTO articles
    (art_id, forum_id, forum_name, author, pub_date, title, subtitle, article) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
'''

URL_APPLEDAILY = 'http://www.appledaily.com.tw/'
URL_BLOGLIST = 'http://www.appledaily.com.tw/appledaily/bloglist/forum/{0}/{1}/'


class AppleForumCrawler():
    """Apple Forum Crawler
    """

    def __init__(self, forum_id):
        self.urlopen_count = 0
        self.author = ''
        self.forum_id = ''
        self.forum_name = ''
        self.init_db()
        self.init_logger(forum_id)

    def init_db(self):
        """init db
        """
        self.conn = sqlite3.connect('source-forum.db')
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_TABLE_ARTICLES)
        self.conn.commit()
        cur.close()

    def init_logger(self, forum_id):
        """init logger
        """
        self.logger = logging.getLogger('forum')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-7s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        ifh = logging.FileHandler(
            'crawler-forum-{0}.log'.format(forum_id), encoding='utf-8')
        ifh.setFormatter(formatter)
        ifh.setLevel(logging.INFO)
        wfh = logging.FileHandler(
            'crawler-forum-{0}-warn.log'.format(forum_id), encoding='utf-8')
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
        if sec is not None and self.urlopen_count % 10 != 0:
            sleep(sec)
            return
        if self.urlopen_count % 100 == 0:
            seconds = randint(30, 90)
            print('(count: {0}, sleep {1} seconds) ['.format(
                self.urlopen_count, seconds), end='')
            for _ in range(0, seconds, 2):
                print(':', end='', flush=True)
                sleep(2)
            print(']')
        elif self.urlopen_count % 10 == 0:
            seconds = randint(5, 9)
            print('(count: {0}, sleep {1} seconds) ['.format(
                self.urlopen_count, seconds), end='')
            for _ in range(seconds):
                print('.', end='', flush=True)
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

    def fetch_article(self, href, title):
        """fetch article
        """
        tokens = href.split('/')
        art_id = tokens[5]
        pub_date = date_iso(tokens[4])
        self.sleep(sec=0)
        url = urljoin(URL_APPLEDAILY, href)
        uri = iri_to_uri(url)
        soup = BeautifulSoup(urlopen(uri), PARSER)
        for br_tag in soup.find_all('br'):
            br_tag.replace_with('\n\n')
        h1_tag = soup.find('h1', {'id': 'h1'})
        h2_tag = soup.find('h2', {'id': 'h2'})
        title = h1_tag.text if h1_tag is not None else ''
        subtitle = h2_tag.text if h2_tag is not None else ''
        cont = ''
        cont_tag = soup.find('div', {'class': 'articulum'})
        if cont_tag is None:
            self.logger.error('      -> article[%s] %s has no content',
                              art_id, href)
            return []
        for ctag in cont_tag.find_all(True, recursive=False):
            if ctag.name == 'p':
                cont += ctag.text
            elif ctag.name == 'h2':
                cont += '## {0}\n\n'.format(ctag.text)
        soup.decompose()
        # art_id, forum_id, forum_name, author, pub_date, title, article
        return [art_id, self.forum_id, self.forum_name, self.author,
                pub_date, title, subtitle, cont]

    def save_article(self, article_values):
        """save article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_ARTICLE, article_values)
        self.conn.commit()
        cur.close()

    def fetch(self, forum_id, count):
        """fetch
        """
        self.forum_id = forum_id
        loop = 9999 if int(count) == -1 else int(count)
        page = 1
        exit_loop = False
        while not exit_loop:
            self.sleep()
            self.logger.info('fetching page %d...', page)
            url = URL_BLOGLIST.format(forum_id, page)
            soup = BeautifulSoup(urlopen(url), PARSER)
            if self.author == '':
                h2_tag = soup.find('h2', {'class': 'auw'})
                if h2_tag is not None:
                    self.forum_name = h2_tag.text.strip()
                    self.author = self.forum_name.split(u'．')[0]
            ul_tag = soup.find('ul', {'class': 'auallt'})
            if ul_tag is None:
                break
            li_tags = ul_tag.find_all('li')
            if li_tags is None or len(li_tags) == 0:
                break
            art_list = []
            for li_tag in li_tags:
                a_tag = li_tag.find('a')
                if a_tag is None:
                    continue
                href = a_tag.get('href')
                title = a_tag.text
                art_list.append((href, title))
            for art_info in art_list:
                tokens = art_info[0].split('/')
                art_id = tokens[5]
                self.logger.info(
                    'fetching article[%s] %s...', art_id, art_info[1])
                if self.contain_article(art_id):
                    self.logger.info(
                        '      -> article[%s] contained and skip', art_id)
                    loop -= 1
                    if loop <= 0:
                        exit_loop = True
                        break
                    continue
                art_values = self.fetch_article(*art_info)
                if len(art_values) == 0:
                    continue
                self.save_article(art_values)
                self.logger.info(
                    '      -> article[%s] %s saved', art_id, art_info[1])
                loop -= 1
                if loop <= 0:
                    exit_loop = True
                    break
            page += 1


def print_usage():
    """Print Usage
    """
    print('usage: {0} <forum id> <count>'.format(sys.argv[0]))
    print('')
    print('    fetch all if <count> == `-1`')
    print('')
    print('ex.    {0} 926953 100'.format(sys.argv[0]))


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print_usage()
        sys.exit(0)
    else:
        CRAWLER = AppleForumCrawler(sys.argv[1])
        CRAWLER.fetch(sys.argv[1], sys.argv[2])
