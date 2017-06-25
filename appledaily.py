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
from urllib.request import urljoin, urlopen

from bs4 import BeautifulSoup

from utils import PARSER, date_iso, iri_to_uri

SQL_CREATE_TABLE_ARTICLES = '''
CREATE TABLE IF NOT EXISTS articles (
    art_id TEXT, pub_date TEXT, category TEXT, section TEXT, title TEXT, subtitle TEXT, article TEXT,
    PRIMARY KEY(art_id)
)
'''
SQL_CREATE_TABLE_DAILIES = '''
CREATE TABLE IF NOT EXISTS dailies (
    id TEXT, sections TEXT, articles TEXT, article_count INTEGER,
    PRIMARY KEY(id)
)
'''
SQL_CONTAIN_ARTICLE = '''
SELECT 1 FROM articles WHERE art_id=?
'''
SQL_CONTAIN_DAILY = '''
SELECT 1 FROM dailies WHERE id=?
'''
SQL_SELECT_DAILY_SECTIONS = '''
SELECT id, sections, articles FROM dailies 
'''
SQL_SELECT_DAILY_SECTIONS_BY_YEAR = '''
SELECT id, sections, articles FROM dailies WHERE id LIKE ?
'''
SQL_INSERT_ARTICLE = '''
INSERT OR IGNORE INTO articles (art_id, pub_date, category, section, title, subtitle, article) VALUES (?, ?, ?, ?, ?, ?, ?)
'''
SQL_INSERT_DAILY = '''
INSERT OR IGNORE INTO dailies (id, sections, articles, article_count) VALUES (?, ?, ?, ?)
'''

URL_APPLEDAILY = 'http://www.appledaily.com.tw/'
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
        cur.execute(SQL_CREATE_TABLE_ARTICLES)
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

    def contain_article(self, art_id):
        """contain_article
        """
        cur = self.conn.cursor()
        cur.execute(SQL_CONTAIN_ARTICLE, [art_id])
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

    def fetch_article(self, href, section_name):
        """fetch_article
        """
        href = href.replace('\r', ' ')
        url = urljoin(URL_APPLEDAILY, href)
        href_token = href.split('/')
        if 'home.appledaily' in href:
            cate, date_str, art_id = 'home', href_token[5], href_token[6]
        else:
            cate, date_str, art_id = href_token[3], href_token[4], href_token[5]
        # self.logger.info('%s, %s, %s', pub_date, art_id, href)
        self.logger.info('fetching article[%s] %s...', art_id, href)
        if self.contain_article(art_id):
            self.logger.info('      -> article[%s] contained and skip', art_id)
            return

        self.sleep(sec=0)
        #  (art_id, pub_date, category, section, title, subtitle, article)
        soup = BeautifulSoup(urlopen(iri_to_uri(url)), PARSER)
        h1_tag = soup.find('h1', {'id': 'h1'})
        h2_tag = soup.find('h2', {'id': 'h2'})
        title = h1_tag.text if h1_tag is not None else ''
        subtitle = h2_tag.text if h2_tag is not None else ''
        cont = ''
        cont_tag = soup.find('div', {'class': 'articulum'})
        for ctag in cont_tag.find_all(True, recursive=False):
            if ctag.name in ('p', 'h2'):
                cont += ctag.text
        soup.decompose()
        pub_date = date_iso(date_str)
        article_values = [art_id, pub_date, cate,
                          section_name, title, subtitle, cont]
        self.insert_article(article_values)
        self.logger.info('      -> article[%s] %s saved', art_id, title)

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

    def fetch_daily_news(self, limit=7):
        """fetch_daily_news
        """
        cur = self.conn.cursor()
        for row in cur.execute(SQL_SELECT_DAILY_SECTIONS):
            # following two lines work on python 3.6, but not python 3.5
            # secs = json.loads(row[1], encoding='utf-8')
            # arts = json.loads(row[2], encoding='utf-8')
            str_secs = row[1].decode('utf-8')
            str_arts = row[2].decode('utf-8')
            secs = json.loads(str_secs)
            arts = json.loads(str_arts)
            for sec in arts:
                for art in arts[sec]:
                    post_size = len(arts[sec][art])
                    if post_size >= limit:
                        sec_name = '{0}/{1}'.format(secs[sec], art)
                        href = arts[sec][art][0][0]
                        # self.logger.info('%s -> %s', sec_name, href)
                        self.fetch_article(href, sec_name)

    def fetch_year_articles(self, year, limit=7):
        """fetch year articles
        """
        cur = self.conn.cursor()
        year_cond = '{0}%'.format(year)
        print(year_cond)
        for row in cur.execute(SQL_SELECT_DAILY_SECTIONS_BY_YEAR, [year_cond]):
            str_secs = row[1].decode('utf-8')
            str_arts = row[2].decode('utf-8')
            secs = json.loads(str_secs)
            arts = json.loads(str_arts)
            for sec in arts:
                for art in arts[sec]:
                    post_size = len(arts[sec][art])
                    if post_size >= limit:
                        sec_name = '{0}/{1}'.format(secs[sec], art)
                        href = arts[sec][art][0][0]
                        # self.logger.info('%s -> %s', sec_name, href)
                        self.fetch_article(href, sec_name)

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
    print('    year [year]    fetch news-post of the year (must in DB)')
    print('    all-dailies    fetch all dailies news-post list (from 2003-05-02)')
    print('    all-news       fetch all news-post in DB')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(0)
    elif sys.argv[1] == 'all-dailies':
        CRAWLER = AppleDailyCrawler()
        CRAWLER.fetch_dailies()
    elif sys.argv[1] == 'all-news':
        CRAWLER = AppleDailyCrawler()
        CRAWLER.fetch_daily_news()
    elif sys.argv[1] == 'year':
        CRAWLER = AppleDailyCrawler()
        CRAWLER.fetch_year_articles(sys.argv[2])
    elif sys.argv[1] == 'test':
        CRAWLER = AppleDailyCrawler()
        # CRAWLER.crawl_article()
        # CRAWLER.crawl_month(2016, 5, 3, 20)

        # CRAWLER.fetch_dailies(step=300)

        # CRAWLER.fetch_day(datetime.datetime.now().date())
        # CRAWLER.fetch_day(datetime.datetime.now().date()-datetime.timedelta(days=1))
        # CRAWLER.fetch_day(datetime.date(2003, 5, 2))

        # CRAWLER.find_all_sections()
        # CRAWLER.analyze_article_count(3)
        # CRAWLER.analyze_article_count(4)
        # CRAWLER.analyze_article_count(5)
        # CRAWLER.analyze_article_count(6)
        # CRAWLER.analyze_article_count(7)

        CRAWLER.fetch_daily_news()
