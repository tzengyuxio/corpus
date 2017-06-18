#!/usr/bin/env python3
"""Hanzi Frequency
"""
import json
import sqlite3
import csv


def is_unihan(char):
    """check a char is hanzi or not
    """
    return ('\u4e00' <= char <= '\u9fff' or
            is_unihan_ext(char))


def is_unihan_ext(char):
    """
    CJK Unified Ideographs Extension A: '\U00003400' <= char <= '\U00004dbf'
    CJK Unified Ideographs Extension B: '\U00020000' <= char <= '\U0002a6df'
    CJK Unified Ideographs Extension C: '\U0002a700' <= char <= '\U0002b73f'
    CJK Unified Ideographs Extension D: '\U0002b740' <= char <= '\U0002b81f'
    CJK Unified Ideographs Extension E: '\U0002b820' <= char <= '\U0002ceaf'
    """
    return ('\u3400' <= char <= '\u4dbf' or  # CJK Unified Ideographs Extension A
            '\U00020000' <= char <= '\U0002a6df' or  # CJK Unified Ideographs Extension B
            '\U0002a700' <= char <= '\U0002ceaf')  # CJK Unified Ideographs Extension C,D,E


def report_summary():
    """report_summary
    """
    all_hanzi_count = 0
    all_hanzi_freq_table = {}
    conn = sqlite3.connect('corpus.db')
    cur = conn.cursor()
    for row in cur.execute('SELECT * FROM corpus'):
        # src = row[0]
        stats = json.loads(row[3])
        for char in stats:
            if char not in all_hanzi_freq_table:
                all_hanzi_freq_table[char] = stats[char]
            else:
                all_hanzi_freq_table[char] += stats[char]
    all_hanzi_count = sum(all_hanzi_freq_table.values())
    print("char size:", all_hanzi_count)
    print("uniq size:", len(all_hanzi_freq_table))

    # save all_hanzi_freq_table
    accum_count = 0
    all_report_table = []
    with open('all-report.csv', 'w', encoding='utf8', newline='') as outfile:
        writer = csv.writer(outfile, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['字頻序號', '字', '擴展', '出現頻次', '出現頻率', '累積頻次', '累積頻率'])
        for idx, item in enumerate(
                sorted(all_hanzi_freq_table.items(), key=lambda x: x[1], reverse=True)):
            is_ext = 'ext' if is_unihan_ext(item[0]) else ''
            accum_count += item[1]
            item = [idx + 1, item[0], is_ext, item[1], item[1] /
                    all_hanzi_count, accum_count, accum_count / all_hanzi_count]
            all_report_table.append(item)
            writer.writerow(item)
        print('[Done] save report file <{0}>'.format('all-report.csv'))


if __name__ == '__main__':
    report_summary()
