#!/usr/bin/python3
# -*- coding: utf-8 -*-
""" Processes the raw data according to business rules """

import scraperwiki
import utils


def _get_donor_amount_figure(contribution, figure_key, usd_amount_key):
    return_value = 0
    for amount in contribution[figure_key][figure_key]:
        return_value += amount.get(usd_amount_key, 0)
    return return_value


def _process_contributions(config):

    def update_progress():
        utils.progress(progress_value, progress_max, prefix='Processing Contributions:', bar_length=50)

    contributions_list = config['collect_result']['json_data']
    progress_max = len(contributions_list)  # Used to track the total number of rows for the progress bar
    progress_value = 0  # Used to track the number of rows processed so far
    update_progress()

    scraperwiki.sql.execute('DELETE FROM _tmp_contributions')

    for contribution in contributions_list:
        scraperwiki.sql.execute('INSERT INTO _tmp_contributions values (' + ','.join('?'*15) + ')', (
            contribution.get('activityDateType'),
            contribution.get('contributionCode'),
            contribution.get('contributionId'),
            contribution.get('countryCode'),
            contribution.get('donor'),
            _get_donor_amount_figure(contribution, 'donorcommitment', 'commitmentamountUSD'),
            _get_donor_amount_figure(contribution, 'donorpledge', 'pledgeAmountUSD'),
            _get_donor_amount_figure(contribution, 'donorreceived', 'receivedamountUSD'),
            _get_donor_amount_figure(contribution, 'donorwriteoff', 'writeoffamountUSD'),
            contribution.get('latestDate'),
            contribution.get('flag'),
            contribution.get('donortype'),
            contribution.get('regionName'),
            contribution.get('statusCode'),
            contribution.get('year')))
        progress_value += 1
        update_progress()

    scraperwiki.sql.execute('DELETE FROM contributions')
    scraperwiki.sql.execute('INSERT INTO contributions SELECT * FROM _tmp_contributions')

    return config


def process(config):
    config['process_result'] = {
        'success': False,
        'messages': []
    }

    _process_contributions(config)

    config['process_result']['success'] = True
    return config


