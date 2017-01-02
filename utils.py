#!/usr/bin/python3
# -*- coding: utf-8 -*-
""" Utility functions """

import hashlib
import requests
import datetime
import sys


def date_from_iso_date(s):
    """Construct a date from an iso date string.

    Supports iso date of the form YYYY-MM-DD.
    Ignores any chars after the date part.
    """
    return datetime.date(year=int(s[0:4]), month=int(s[5:7]), day=int(s[8:10]))


def generate_hash(filename):
    """Generate hash of a file.
    """
    h = hashlib.sha1()
    with open(filename, 'rb') as f:
        buf = f.read()
        h.update(buf)
    return h.hexdigest()


def download_file(url, local_filename):
    """Downloads a file.
    """
    if not local_filename:
        local_filename = url.split('/')[-1]
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return local_filename


def progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    try:
        format_str = "{0:." + str(decimals) + "f}"
        percents = format_str.format(100 * (iteration / float(total)))
        filled_length = int(round(bar_length * iteration / float(total)))
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),
        if iteration == total:
            sys.stdout.write('\n')
        sys.stdout.flush()
    except:
        pass
