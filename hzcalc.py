#!/usr/bin/env python3
"""Hanzi calculation
"""

import csv
import json
import sqlite3
import sys

from utils import datetime_iso, is_unihan, is_unihan_ext


SQL_CREATE_ARTICLES = '''
CREATE TABLE IF NOT EXISTS articles (
    src TEXT, idx TEXT, pub_date TEXT, raw_txt TEXT, stats TEXT, hanzi_cnt INTEGER, hanzi_sum INTEGER,
    PRIMARY KEY(src, idx)
)
'''
SQL_INSERT_ARTICLES = '''
INSERT OR IGNORE INTO articles VALUES (?, ?, ?, ?, ?, ?, ?)
'''
SQL_SELECT_ARTICLES = '''
SELECT * FROM articles
'''
SQL_SELECT_BY_FORUM_ID = '''
SELECT art_id, title, article FROM articles WHERE forum_id=? AND article like "%男女%" LIMIT 500
'''


class HanziCalculator():
    """Hanzi Calculator
    """

    def __init__(self, db_name='hzfreq.db'):
        # init db
        self.conn = sqlite3.connect(db_name)
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_ARTICLES)
        self.conn.commit()
        cur.close()

    def save_report(self, all_hz_freq, report_file):
        """save report to csv file
        """
        accum_count = 0
        all_hz_sum = sum(all_hz_freq.values())
        with open(report_file, 'w', encoding='utf8', newline='') as fout:
            writer = csv.writer(fout, delimiter=',', quotechar='"',
                                quoting=csv.QUOTE_MINIMAL)
            writer.writerow(
                ['字頻序號', '字', '擴展', '出現頻次', '出現頻率', '累積頻次', '累積頻率'])
            for i, item in enumerate(sorted(all_hz_freq.items(),
                                            key=lambda x: x[1], reverse=True)):
                is_ext = 'ext' if is_unihan_ext(item[0]) else ''
                accum_count += item[1]
                writer.writerow([i + 1, item[0], is_ext,
                                 item[1], item[1] / all_hz_sum,
                                 accum_count, accum_count / all_hz_sum])

    def print_result(self, header, text_cnt, uniq_cnt, char_sum):
        """print result
        """
        print('')
        print('### [{0}] ##############################'.format(header))
        print('  total text cnt: {0:>12,}'.format(text_cnt))
        print('  hanzi uniq cnt: {0:>12,}'.format(uniq_cnt))
        print('  hanzi char sum: {0:>12,}'.format(char_sum))
        print('')

    def calc_all(self, db_file, report_file):
        """calc all in freq db
        """
        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        all_hz_freq = {}
        art_cnt = 0
        for i, row in enumerate(cur.execute(SQL_SELECT_ARTICLES)):
            print('{0} INFO {1:,} calc article[{2}]... hanzi cnt/sum: {3}/{4}'.format(
                datetime_iso(), i, row['idx'], row['hanzi_cnt'], row['hanzi_sum']))
            chr_freq = json.loads(row['stats'])
            for char in chr_freq:
                if char in all_hz_freq:
                    all_hz_freq[char] += chr_freq[char]
                else:
                    all_hz_freq[char] = chr_freq[char]
            art_cnt += 1

        self.save_report(all_hz_freq, report_file)
        self.print_result(report_file, art_cnt,
                          len(all_hz_freq), sum(all_hz_freq.values()))

    def calc_articles(self, src, src_db_name, sql_query):
        """calc hanzi freq articles
        """
        art_cnt = 0
        src_db = sqlite3.connect(src_db_name)
        src_db.row_factory = sqlite3.Row
        src_cur = src_db.cursor()
        all_hz_freq = {}
        for i, row in enumerate(src_cur.execute(sql_query)):
            art_cnt += 1
            idx = row['art_id']
            pub_date = row['pub_date']
            raw_text = row['raw_text']
            chr_freq = {}
            for char in raw_text:
                if char in chr_freq:
                    chr_freq[char] += 1
                else:
                    chr_freq[char] = 1
            hz_freq = {k: v for k, v in chr_freq.items() if is_unihan(k)}
            for k in hz_freq:
                if k in all_hz_freq:
                    all_hz_freq[k] += hz_freq[k]
                else:
                    all_hz_freq[k] = hz_freq[k]
            stats = json.dumps(hz_freq, ensure_ascii=False,
                               sort_keys=True).encode('utf-8')
            hanzi_cnt = len(hz_freq)
            hanzi_sum = sum(hz_freq.values())
            rec = [src, idx, pub_date, raw_text, stats, hanzi_cnt, hanzi_sum]
            cur = self.conn.cursor()
            cur.execute(SQL_INSERT_ARTICLES, rec)
            print('{0} INFO {1:05} calc article[{2}]... hanzi cnt/sum: {3}/{4}'.format(
                datetime_iso(), i + 1, idx, hanzi_cnt, hanzi_sum))
        src_cur.close()
        self.conn.commit()

        self.save_report(all_hz_freq, 'report-{0}.csv'.format(src))
        self.print_result(src, art_cnt,
                          len(all_hz_freq), sum(all_hz_freq.values()))

    def dump_forum(self, src_db_name, fid):
        """dump forum to text file
        """
        txt = ''
        conn = sqlite3.connect(src_db_name)
        cur = conn.cursor()
        for row in cur.execute(SQL_SELECT_BY_FORUM_ID, [fid]):
            # txt += '{0}\n\n{1}\n\n\n'.format(row[1], row[2])
            txt += '{0}\n'.format(row[2])
        filename = 'dump-forum-{0}.txt'.format(fid)
        with open(filename, 'w', encoding='utf8') as fout:
            fout.write(txt)
        print('\nfile {0} saved\n'.format(filename))


if __name__ == '__main__':
    if sys.argv[1] == 'all':
        CALC = HanziCalculator()
        CALC.calc_all('hzfreq.db', 'report-all.csv')
    elif sys.argv[1] == 'forum':
        CALC = HanziCalculator(db_name='hzfreq-forum.db')
        FID = sys.argv[2]
        CALC.calc_articles(
            'appledaily.forum.{0}'.format(FID),
            'source-forum.db',
            '''SELECT art_id, pub_date,
               title || x'0a' || subtitle || x'0a0a' || article AS raw_text
               FROM articles WHERE forum_id="{0}"'''.format(FID)
        )
    elif sys.argv[1] == 'apple':
        CALC = HanziCalculator()
        CALC.calc_articles(
            'news.apple',
            'source-appledaily.db',
            '''SELECT art_id, pub_date,
               title || x'0a' || subtitle || x'0a0a' || article AS raw_text
               FROM articles'''
        )
    elif sys.argv[1] == 'books':
        CALC = HanziCalculator()
        CALC.calc_articles(
            'books',
            'source-books.db',
            '''SELECT book_no AS art_id, pub_date, title || article AS raw_text
               FROM articles'''
        )
    elif sys.argv[1] == 'cnyes':
        CALC = HanziCalculator()
        CALC.calc_articles(
            'mag.cnyes',
            'source-magcnyes.db',
            '''SELECT art_id, pub_date,
               full_title || x'0a0a' || article AS raw_text
               FROM articles'''
        )
    elif sys.argv[1] == 'yahoo':
        CALC = HanziCalculator()
        CALC.calc_articles(
            'news.yahoo',
            'source-newsyahoo.db',
            '''SELECT id AS art_id, pub_date,
               title || x'0a0a' || article AS raw_Text
               FROM articles'''
        )
    elif sys.argv[1] == 'wiki':
        CALC = HanziCalculator()
        CALC.calc_articles(
            'wikipedia',
            'source-wikipedia.db',
            '''SELECT title AS art_id, open_date AS pub_date,
               title || x'0a0a' || article AS raw_text
               FROM articles'''
        )
    elif sys.argv[1] == 'dump':
        CALC = HanziCalculator(db_name='hzfreq-forum.db')
        FID = sys.argv[2]
        CALC.dump_forum('source-forum.db', FID)
