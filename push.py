#!/usr/bin/python3
# -*- coding: utf-8 -*-
""" Push the data to HDX """

from hdx.data.dataset import Dataset
import datetime


def _set_dataset_date(config):
    d = Dataset.read_from_hdx(config,'1a78d371-6d67-4208-a324-f4be6d7182c6')
    d.set_dataset_date_from_datetime(datetime.datetime.now())
    d.update_in_hdx(update_gallery=False, update_resources=False)


def push(config):
    _set_dataset_date(config)
    return config
