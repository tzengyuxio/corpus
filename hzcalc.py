#!/usr/bin/env python3
"""Hanzi calculation
"""

import json
import sqlite3
from utils import is_unihan, datetime_iso

SQL_CREATE_ARTICLES = '''
CREATE TABLE IF NOT EXISTS articles (
    src TEXT, idx TEXT, pub_date TEXT, raw_txt TEXT, stats TEXT, hanzi_cnt INTEGER, hanzi_sum INTEGER,
    PRIMARY KEY(src, idx)
)
'''

SQL_INSERT_ARTICLES = '''
INSERT OR IGNORE INTO articles VALUES (?, ?, ?, ?, ?, ?, ?)
'''


class HanziCalculator():
    """Hanzi Calculator
    """

    def __init__(self):
        # init db
        self.conn = sqlite3.connect('hzfreq.db')
        cur = self.conn.cursor()
        cur.execute(SQL_CREATE_ARTICLES)
        self.conn.commit()
        cur.close()

    def calc_forum(self):
        """calc forum db
        """
        art_cnt = 0
        src = 'appledaily.forum.926953'
        src_db = sqlite3.connect('source-forum.db')
        src_cur = src_db.cursor()
        total_hz_freq = {}
        for i, row in enumerate(src_cur.execute('SELECT * FROM articles')):
            art_cnt += 1
            idx = row[0]
            pub_date = row[4]
            raw_text = '{0}\n{1}\n\n{2}'.format(row[5], row[6], row[7])
            chr_freq = {}
            for char in raw_text:
                if char in chr_freq:
                    chr_freq[char] += 1
                else:
                    chr_freq[char] = 1
            hz_freq = {k: v for k, v in chr_freq.items() if is_unihan(k)}
            for k in hz_freq:
                if k in total_hz_freq:
                    total_hz_freq[k] += hz_freq[k]
                else:
                    total_hz_freq[k] = hz_freq[k]
            stats = json.dumps(hz_freq, ensure_ascii=False,
                               sort_keys=True).encode('utf-8')
            hanzi_cnt = len(hz_freq)
            hanzi_sum = sum(hz_freq.values())
            rec = [src, idx, pub_date, raw_text, stats, hanzi_cnt, hanzi_sum]
            cur = self.conn.cursor()
            cur.execute(SQL_INSERT_ARTICLES, rec)
            print('{0} INFO {1:05} calc article[{2}]... cnt:{3}/sum:{4}'.format(
                datetime_iso(), i, idx, hanzi_cnt, hanzi_sum))
        src_cur.close()
        self.conn.commit()
        print('----------------------------------------')
        print('total text count: {0:>12,}'.format(art_cnt))
        print('hanzi unique cnt: {0:>12,}'.format(len(total_hz_freq)))
        print('hanzi total sum:  {0:>12,}'.format(sum(total_hz_freq.values())))


if __name__ == '__main__':
    CALC = HanziCalculator()
    CALC.calc_forum()
