#!/usr/bin/env python3
"""utilities
"""
from calendar import monthrange
from datetime import datetime

PARSER = 'html.parser'


def datetime_iso():
    """datetime_iso
    """
    return datetime.now().replace(microsecond=0).isoformat(' ')


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
    return ('\u3400' <= char <= '\u4dbf' or          # CJK Extension A
            '\U00020000' <= char <= '\U0002a6df' or  # CJK Extension B
            '\U0002a700' <= char <= '\U0002ceaf')    # CJK Extension C,D,E


def month_range(year, month):
    """month_range
    """
    first, last = monthrange(year, month)
    first_date = '{0:04}-{1:02}-{2:02}'.format(year, month, first)
    last_date = '{0:04}-{1:02}-{2:02}'.format(year, month, last)
    return first_date, last_date
