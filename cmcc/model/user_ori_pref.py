# -*- coding: utf-8 -*-
import os
import pickle
import logging
import sys
from collections import defaultdict, OrderedDict
from data_process.data_process import DataProcessor


class UserOriFeature(object):
    def __init__(self, config):
        self.config = config
        self.processor = DataProcessor(config)
        self.mode = config['mode']
        self.data_delim = config['model']['user_ori_fea_delim']  # user_ori_fea_delim = "€€"
        self.user_ori_fea_delim = config['model']['user_ori_fea_delim']
        self.ori_fea_lens = config['model']["ori_fea_lens"]  # ori_fea_lens
        self.hive_dir = config["data_process"]["hive_dir"]
        self.user_indi_map = defaultdict(OrderedDict)  # 记录用户偏好

    def calculate(self):
        values = self.processor.get_tv_user_feature()
        self.user_indi_map = values
        logging.getLogger('calculate').info(f"get pref userid lens {len(self.user_indi_map)}, {self.user_indi_map}")
        return self.user_indi_map


