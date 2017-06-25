#!/usr/bin/env python3
"""Crawler of appledaily
"""

import json
import logging
import sqlite3
import sys
import datetime
from random import randint
from time import sleep
from urllib.error import HTTPError
from urllib.request import urlopen

from bs4 import BeautifulSoup

from utils import PARSER

SQL_CREATE_TABLE_ARTICLES = '''
CREATE TABLE IF NOT EXISTS articles (
    id TEXT, author TEXT, publisher TEXT, pub_date TEXT, url TEXT, title TEXT, article TEXT,
    PRIMARY KEY(id)
)
'''
SQL_CREATE_TABLE_DAILIES = '''
CREATE TABLE IF NOT EXISTS dailies (
    id TEXT, sections TEXT, articles TEXT, article_count INTEGER,
    PRIMARY KEY(id)
)
'''
SQL_CONTAIN_ARTICLE = '''
SELECT 1 FROM articles WHERE url=?
'''
SQL_CONTAIN_DAILY = '''
SELECT 1 FROM dailies WHERE id=?
'''
SQL_SELECT_DAILY_SECTIONS = '''
SELECT id, sections, articles FROM dailies 
'''
SQL_INSERT_ARTICLE = '''
INSERT OR IGNORE INTO articles (id, author, publisher, pub_date, url, title, article) VALUES (?, ?, ?, ?, ?, ?, ?)
'''
SQL_INSERT_DAILY = '''
INSERT OR IGNORE INTO dailies (id, sections, articles, article_count) VALUES (?, ?, ?, ?)
'''

URL_ARCHIVE = 'http://www.appledaily.com.tw/appledaily/archive/{0}'


class AppleDailyCrawler():
    """Crawler of apple daily
    """

    def __init__(self):
        self.urlopen_count = 0
        self.init_db()
        self.init_logger()
        self.logger.info(
            '---- [AppleDailyCrawler] ---------------------------')

    def init_db(self):
        """init db
        """
        self.conn = sqlite3.connect('source-appledaily.db')
        cur = self.conn.cursor()
        # cur.execute(SQL_CREATE_TABLE_ARTICLES)
        cur.execute(SQL_CREATE_TABLE_DAILIES)
        self.conn.commit()
        cur.close()

    def init_logger(self):
        """init logger
        """
        self.logger = logging.getLogger('appledaily')
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)-7s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

        ifh = logging.FileHandler('crawler-appledaily.log', encoding='utf-8')
        ifh.setFormatter(formatter)
        ifh.setLevel(logging.INFO)
        wfh = logging.FileHandler(
            'crawler-appledaily-warn.log', encoding='utf-8')
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

    def contain_article(self, link):
        """contain_article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_CONTAIN_ARTICLE, [link])
        result = True if cur.fetchone() is not None else False
        cur.close()
        return result

    def contain_daily(self, the_day):
        """contain_daily
        """
        cur = self.conn.cursor()
        cur.execute(SQL_CONTAIN_DAILY, [the_day])
        result = True if cur.fetchone() is not None else False
        cur.close()
        return result

    def insert_daily(self, daily_values):
        """insert_daily
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_DAILY, daily_values)
        self.conn.commit()
        cur.close()

    def insert_article(self, article_values):
        """insert article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_INSERT_ARTICLE, article_values)
        self.conn.commit()
        cur.close()

    def fetch_article(self, url):
        """fetch_article
        """
        self.logger.info('fetching article(%s)...', url)
        if self.contain_article(url):
            self.logger.info('      -> already saved link: %s', url)
            return
        self.sleep()
        try:
            soup = BeautifulSoup(urlopen(url), PARSER)
            title = soup.find('header').text
            art = soup.find('article').text
            uuid = soup.find('article')['data-uuid']
            pub_date = soup.find('time')['datetime'][:10]
            author_tag = soup.find('div', {'class': 'author'})
            provdr_tag = soup.find('span', {'class': 'provider-link'})
            author = author_tag.text if author_tag != None else ''
            provider = provdr_tag.text if provdr_tag != None else ''
            soup.decompose()
            article_values = [uuid, author, provider,
                              pub_date, url, title, art]
            self.insert_article(article_values)
            self.logger.info(
                '      -> [%s] by "%s|%s" at %s', title, provider, author, pub_date)
        except HTTPError as err:
            self.logger.error(
                '      -> article(%s) fetch fail: %s', url, err)
        except KeyError as err:
            self.logger.error(
                '      -> article(%s) fetch fail: KeyError: %s', url, err)
        except AttributeError as err:
            self.logger.error(
                '      -> article(%s) fetch fail: AttributeError: %s', url, err)

    def fetch_articles(self):
        """fetch_articles
        """
        cur = self.conn.cursor()
        for row in cur.execute(SQL_SELECT_DAILY_SECTIONS):
            pick_links = json.loads(row[1])
            for url in pick_links:
                self.fetch_article(url)

    def fetch_daily_article_tag(self, article_tag):
        """fetch_daily_article_tag
        """
        name = article_tag.h2.text
        li_tags = article_tag.find_all('li')
        news_list = []
        for li_tag in li_tags:
            a_tag = li_tag.a
            if a_tag is None:
                continue
            title = a_tag.text if 'title' not in a_tag else a_tag['title']
            news_list.append((a_tag['href'], title))
        return (name, news_list)

    def fetch_day(self, the_day):
        """fetch_day
        """
        self.logger.info('fetching daily[%s]', the_day)
        str_day = the_day.strftime('%Y%m%d')
        if self.contain_daily(str_day):
            self.logger.info('      -> daily[%s] contained and skip', the_day)
            return
        self.sleep(sec=0)
        url = URL_ARCHIVE.format(str_day)
        soup = BeautifulSoup(urlopen(url), PARSER)
        div = soup.find('div', {'class': 'abdominis'})
        if div is None:
            self.logger.error(
                '      -> daily[%s] Cannot find <div> with class "abdominis"', the_day)
            soup.decompose()
            return
        sections = {}
        articles = {}
        curr_sec = ''
        post_cnt = 0
        art_tag_cnt = 0
        for child in div.find_all(True, recursive=False):
            if child.name == 'section':
                curr_sec = child.get('id')
                sec_name = child.header.h1.text.strip()
                self.logger.debug('SECTION [%s](%s)', sec_name, curr_sec)
                sections[curr_sec] = sec_name
                articles[curr_sec] = {}
                for article in child.find_all('article'):
                    art_name, art_news = self.fetch_daily_article_tag(article)
                    art_tag_cnt += 1
                    post_cnt += len(art_news)
                    self.logger.debug(
                        '    ARTICLE [%s] has %d post', art_name, len(art_news))
                    articles[curr_sec][art_name] = art_news
            if child.name == 'article':
                art_name, art_news = self.fetch_daily_article_tag(child)
                art_tag_cnt += 1
                post_cnt += len(art_news)
                self.logger.debug(
                    '    ARTICLE [%s] has %d post', art_name, len(art_news))
                articles[curr_sec][art_name] = art_news
        soup.decompose()
        sections_str = json.dumps(sections, ensure_ascii=False).encode('utf-8')
        articles_str = json.dumps(articles, ensure_ascii=False).encode('utf-8')
        daily_values = [str_day, sections_str, articles_str, post_cnt]
        self.insert_daily(daily_values)
        self.logger.info(
            '      -> daily[%s] has %d posts in %d/%d sections',
            the_day, post_cnt, len(sections), art_tag_cnt)

    def fetch_dailies(self, step=1):
        """fetch_all
        """
        start = datetime.date(2003, 5, 2)
        end = datetime.datetime.now().date()
        the_day = start
        while True:
            if the_day > end:
                break
            self.fetch_day(the_day)
            the_day += datetime.timedelta(days=step)

    def find_all_sections(self):
        """find_all_sections
        """
        sections = {}
        cur = self.conn.cursor()
        for row in cur.execute(SQL_SELECT_DAILY_SECTIONS):
            dsecs = json.loads(row[1])
            for sec in dsecs:
                if sec not in sections:
                    sections[sec] = [dsecs[sec]]
                else:
                    if dsecs[sec] not in sections[sec]:
                        sections[sec].append(dsecs[sec])
        cur.close()
        for sec in sections:
            self.logger.info('section[%s]: %s', sec, ','.join(sections[sec]))

    def analyze_article_count(self, limit):
        """analyze article count
        """
        daily_count = {}
        cur = self.conn.cursor()
        for row in cur.execute(SQL_SELECT_DAILY_SECTIONS):
            day_count = 0
            arts = json.loads(row[2])
            for sec in arts:
                # self.logger.info('%s', sec)
                for art in arts[sec]:
                    # self.logger.info('    %s', art)
                    post_size = len(arts[sec][art])
                    if post_size >= limit:
                        day_count += 1
                    # for post in arts[sec][art]:
                    #     self.logger.info('        %s', post[0])
            daily_count[row[0]] = day_count
            # self.logger.info('%s -> %d posts', row[0], day_count)
        cur.close()
        avg = sum([daily_count[x] for x in daily_count]) / len(daily_count)
        self.logger.info('Avg. day count: %.2f when section minimal limit is %d',
                         avg, limit)


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
        CRAWLER = AppleDailyCrawler()
        CRAWLER.fetch_dailies()
    elif sys.argv[1] == 'test':
        CRAWLER = AppleDailyCrawler()
        # CRAWLER.crawl_article()
        # CRAWLER.crawl_month(2016, 5, 3, 20)

        # CRAWLER.fetch_dailies(step=300)

        # CRAWLER.fetch_day(datetime.datetime.now().date())
        # CRAWLER.fetch_day(datetime.datetime.now().date()-datetime.timedelta(days=1))
        # CRAWLER.fetch_day(datetime.date(2003, 5, 2))

        # CRAWLER.find_all_sections()
        CRAWLER.analyze_article_count(3)
        CRAWLER.analyze_article_count(4)
        CRAWLER.analyze_article_count(5)
        CRAWLER.analyze_article_count(6)
        CRAWLER.analyze_article_count(7)
