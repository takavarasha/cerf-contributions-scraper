#!/usr/bin/python3
# -*- coding: utf-8 -*-
""" Wrapper function to run HDX scrapers """


import logging
import collect
import process
import push
from hdx.configuration import Configuration
import scraperwiki
import datetime

logger = logging.getLogger(__name__)


def main(config):

    run_timestamp = datetime.datetime.now()

    collect.collect(config)

    if config.get('collect_result').get('file_changed'):
        process.process(config=config)
        push.push(config=config)
        scraperwiki.sql.save_var(u'hash',config['collect_result']['file_hash'])
        scraperwiki.sql.save_var('last_update', run_timestamp)

    last_update = scraperwiki.sql.get_var('last_update')
    # The following is necessary because of a bug in scraperwiki.sql.get_var which always returns None even when a
    # a value is provided in the default parameter.
    if not last_update:
        last_update = run_timestamp

    message = 'Checked: {0}, Updated: {1}'.format(run_timestamp, last_update)
    scraperwiki.status('ok', message=message)
    print(message)

if __name__ == '__main__':
    main(config=Configuration(hdx_site='prod', project_config_dict={}))

