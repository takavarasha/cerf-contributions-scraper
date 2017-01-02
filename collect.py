#!/usr/bin/python3
# -*- coding: utf-8 -*-
""" Collects data from the remote system """

import utils
import scraperwiki
import json
import codecs

def collect(config):
    json_file_path = utils.download_file(
        'http://cerfgms-webapi.unocha.org/v1/donorcontribution.json',
        'raw.json'
    )
    json_data = json.loads(codecs.open(json_file_path, encoding='utf_8').read())
    file_hash = utils.generate_hash(json_file_path)
    last_hash = scraperwiki.sqlite.get_var(u'hash')
    file_changed = (last_hash != file_hash)

    config['collect_result'] = {
        'success': True,
        'message': '',
        'json_file': json_file_path,
        'json_data': json_data,
        'json_data_as_string': json.dumps(json_data),
        'file_hash': file_hash,
        'last_hash': last_hash,
        'file_changed': file_changed
    }
    return config
