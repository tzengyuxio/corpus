#!/usr/bin/env python3
"""utilities
"""
import re
from urllib import parse
from calendar import monthrange
from datetime import datetime

PARSER = 'lxml'  # 'html.parser'


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


def date_iso(date_str):
    """convert '20170629' to '2017-06-29'
    """
    return '{0}-{1}-{2}'.format(date_str[0:4], date_str[4:6], date_str[6:8])


def url_encode_non_ascii(byt):
    """urlEncodeNonAscii
    """
    return re.sub('[\x80-\xFF]', lambda c: '%%%02x' % ord(c.group(0)), byt)


def iri_to_uri(iri):
    """iri to uri
    """
    parts = parse.urlsplit(iri)
    parts = list(parts)
    parts[2] = parse.quote(parts[2])
    parts[3] = parse.quote(parts[3]) # this hack only work for appledaily
    return parse.urlunsplit(parts)
    # return parse.urlunparse(
    #     part.encode('idna') if parti == 1 else url_encode_non_ascii(
    #         part.encode('utf-8'))
    #     for parti, part in enumerate(parts)
    # )
