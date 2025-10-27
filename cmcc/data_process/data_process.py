import os
import logging
from collections import defaultdict, OrderedDict
import pickle


class DataProcessor:
    def __init__(self, config):
        self.config = config
        self.mode = config['mode']

        if self.mode == 'local':
            self.data_dir = self.config['data_process']['local_dir']
            self.user_number_table_name = self.config['data_process']['local_user_number_table_name']
            self.call_table_name = self.config['data_process']['local_call_table_name']
            self.tv_user_table = self.config['model']['local_tv_user_feature_table']
            self.inter_dir = self.config['output']['local_inter_save_dir']
        else:
            self.data_dir = self.config['data_process']['hive_dir']
            self.user_number_table_name = self.config['data_process']['user_number_table_name']
            self.call_table_name = self.config['data_process']['call_table_name']
            self.tv_user_table = self.config['model']['tv_user_feature_table']
            self.inter_dir = self.config['output']['inter_save_dir']

        self.model_path = self.config['output']['local_graph_model_save_path'] if self.mode == 'local' else self.config['output']['graph_model_save_path']
        self.data_delim = self.config['data_process']['data_delim']
        self.user_table_dim = len(self.config['data_process']['user_features'])
        self.call_table_dim = len(self.config['data_process']['call_features'])
        self.user_ori_fea_delim = config['model']['user_ori_fea_delim']
        self.ori_fea_lens = config['model']["ori_fea_lens"]  # ori_fea_lens
        self.monthid = self.config['monthid']  # 可能使用多个monthid
        self.province = self.config['province']
        self.msisdn_user_map_path = self.config['output']['msisdn_user_map_path']
        assert len(self.monthid) > 0 and len(self.province) > 0

    def load_hive_user(self):
        users = []
        msisdn_map = {}
        monthid = self.monthid
        command = "hdfs dfs -ls " + self.data_dir + str(self.user_number_table_name) + "/monthid=" + str(
            monthid) + "/province=" + self.province + " | awk '{print $NF}'"
        user_hive_path_list = os.popen(command).read().strip().split("\n")[1:]
        user_hive_path_list = sorted(user_hive_path_list)
        logging.getLogger('data_process').info(
            f"In monthid {monthid}, province {self.province}, user-number has {len(user_hive_path_list)} hive tables: {user_hive_path_list}")
        for path in user_hive_path_list[:]:
            logging.getLogger('data_process').info(f"{path}")
            command = "hdfs dfs -text " + path
            contents = os.popen(command).read().strip().split("\n")
            for line in contents:
                value_list = line.split(self.data_delim)
                if len(value_list) < self.user_table_dim:
                    continue
                users.append(tuple(value_list))
                msisdn, user_id = value_list[0], value_list[1]
                msisdn_map[msisdn] = user_id
                # node_set.add(value_list[0])

        map_path = self.model_path + self.msisdn_user_map_path
        with open(map_path, "wb") as f:
            pickle.dump(msisdn_map, f)
            logging.getLogger('data_process').info(f"Succeed save msisdn_user_map to {map_path}")
        return users

    def load_hive_call(self):
        calls = []
        monthid = self.monthid
        command = "hdfs dfs -ls " + self.data_dir + str(self.call_table_name) + "/mothid=" + str(
            monthid) + "/province=" + self.province + " | awk '{print $NF}'"
        call_hive_path_list = os.popen(command).read().strip().split("\n")[1:]
        call_hive_path_list = sorted(call_hive_path_list)
        logging.getLogger('data_process').info(f"In monthid {monthid}, province {self.province}, call has {len(call_hive_path_list)} hive tables: {call_hive_path_list}")
        for path in call_hive_path_list[:]:
            logging.getLogger('data_process').info(f"{path}")
            command = "hdfs dfs -text " + path
            contents = os.popen(command).read().strip().split("\n")
            for line in contents:
                value_list = line.split(self.data_delim)
                if len(value_list) < self.call_table_dim:
                    continue
                calls.append(tuple(value_list))
        calls = list(set(calls))

        return calls

    def load_hive_tv_user_feature(self):
        # tv_user_features = []
        user_indi_map = defaultdict(OrderedDict)
        command = "hdfs dfs -ls " + self.data_dir + str(self.tv_user_table) + "/monthid=" + str(self.monthid) \
                  + "/province=" + str(self.province) + " | awk '{print $NF}'"
        logging.getLogger('data_process').info(command)

        hive_path_list = os.popen(command).read().strip().split("\n")
        logging.info(str(self.tv_user_table) + " have " + str(len(hive_path_list[1:])) + " files")
        total_counts = 0
        logging.getLogger('data_process').info(
            f"In monthid {self.monthid}, province {self.province}, tv_user_feature has {len(hive_path_list)} hive tables: {hive_path_list}")
        for hive_path in hive_path_list[1:]:
            logging.getLogger('data_process').info(f"{hive_path}")
            command = "hdfs dfs -text " + hive_path
            contents = os.popen(command).read().strip().split("\n")
            for content in contents:
                value_list = content.strip().split(self.user_ori_fea_delim)
                if len(value_list) < self.ori_fea_lens:
                    continue
                # tv_user_features.append(value_list)
                user_oriid = value_list[0]  # user_id
                wady_onnet_flux = float(value_list[22])
                if wady_onnet_flux == 'null' or wady_onnet_flux == '\\N' or wady_onnet_flux == 0:
                    wady_onnet_flux_pref = 0
                else:
                    wady_onnet_flux_pref = int(wady_onnet_flux)

                nwady_onnet_flux = float(value_list[23])
                if nwady_onnet_flux == 'null' or nwady_onnet_flux == '\\N' or nwady_onnet_flux == 0:
                    nwady_onnet_flux_pref = 0
                else:
                    nwady_onnet_flux_pref = int(nwady_onnet_flux)

                flux_fee = float(value_list[27])
                if flux_fee == 'null' or flux_fee == '\\N' or flux_fee == 0:
                    flux_fee_pref = 0
                else:
                    flux_fee_pref = int(flux_fee)

                pack_mon = float(value_list[32])
                if pack_mon == 'null' or pack_mon == '\\N' or pack_mon == 0:
                    pack_mon_pref = 0
                else:
                    pack_mon_pref = int(pack_mon)

                user_indi_map[user_oriid]["wady_onnet_flux_pref"] = wady_onnet_flux_pref
                user_indi_map[user_oriid]["nwady_onnet_flux_pref"] = nwady_onnet_flux_pref
                user_indi_map[user_oriid]["flux_fee_pref"] = flux_fee_pref
                user_indi_map[user_oriid]["pack_mon_pref"] = pack_mon_pref

        logging.getLogger('data_process').info(f'succeed to get tv_user_features')
        return user_indi_map

    def load_local_user(self):
        users = []
        msisdn_map = {}
        user_path = self.data_dir + self.user_number_table_name

        with open(user_path, "r", encoding='utf-8') as fr:
            for line in fr.readlines():
                value_list = line.split(self.data_delim)
                value_list[-1] = value_list[-1].split("\n")[0]
                if len(value_list) < self.user_table_dim:
                    continue
                users.append(tuple(value_list))
                msisdn, user_id = value_list[0], value_list[1]
                msisdn_map[msisdn] = user_id
            fr.close()

        map_path = self.model_path + self.msisdn_user_map_path
        with open(map_path, "wb") as f:
            pickle.dump(msisdn_map, f)
            logging.getLogger('data_process').info(f"Succeed save msisdn_user_map to {map_path}")
        return users

    def load_local_call(self):
        calls = []
        call_path = self.data_dir + self.call_table_name
        with open(call_path, "r", encoding='utf-8') as fr:
            for line in fr.readlines():
                value_list = line.split(self.data_delim)
                value_list[-1] = value_list[-1].split("\n")[0]
                if len(value_list) < self.call_table_dim:
                    continue
                value_list = value_list[:2]
                calls.append(tuple(value_list))
                fr.close()

        calls = list(set(calls))
        return calls

    def load_local_tv_user_feature(self):
        # tv_user_features = []
        user_indi_map = defaultdict(OrderedDict)
        tv_path = self.data_dir + self.tv_user_table
        with open(tv_path, "r", encoding='utf-8') as fr:
            for line in fr.readlines():
                value_list = line.split(self.data_delim)
                value_list[-1] = value_list[-1].split("\n")[0]
                if len(value_list) < self.ori_fea_lens:
                    continue
                user_oriid = value_list[0]  #value_list[1]
                wady_onnet_flux = float(value_list[22])
                if wady_onnet_flux == 'null' or wady_onnet_flux == '\\N' or wady_onnet_flux == 0:
                    wady_onnet_flux_pref = 0
                else:
                    wady_onnet_flux_pref = int(wady_onnet_flux)

                nwady_onnet_flux = float(value_list[23])
                if nwady_onnet_flux == 'null' or nwady_onnet_flux == '\\N' or nwady_onnet_flux == 0:
                    nwady_onnet_flux_pref = 0
                else:
                    nwady_onnet_flux_pref = int(nwady_onnet_flux)

                flux_fee = float(value_list[27])
                if flux_fee == 'null' or flux_fee == '\\N' or flux_fee == 0:
                    flux_fee_pref = 0
                else:
                    flux_fee_pref = int(flux_fee)

                pack_mon = float(value_list[32])
                if pack_mon == 'null' or pack_mon == '\\N' or pack_mon == 0:
                    pack_mon_pref = 0
                else:
                    pack_mon_pref = int(pack_mon)

                user_indi_map[user_oriid]["wady_onnet_flux_pref"] = wady_onnet_flux_pref
                user_indi_map[user_oriid]["nwady_onnet_flux_pref"] = nwady_onnet_flux_pref
                user_indi_map[user_oriid]["flux_fee_pref"] = flux_fee_pref
                user_indi_map[user_oriid]["pack_mon_pref"] = pack_mon_pref

        return user_indi_map

    def get_user_call(self):
        if self.mode == 'local':
            logging.getLogger('data_process').info(f'Starts to process local data!')
            users = self.load_local_user()
            calls = self.load_local_call()
        else:
            logging.getLogger('data_process').info(f'Starts to process hive data!')
            users = self.load_hive_user()
            calls = self.load_hive_call()

        logging.getLogger('data_process').info(f'Get {len(users)} users, {len(calls)} calls!')
        return users, calls

    def get_user(self):
        user_path = self.model_path + f"user_{self.province}_{self.monthid}_{self.mode}.pkl"
        if os.path.exists(user_path):
            with open(user_path, "rb") as f:
                users = pickle.load(f)

            logging.getLogger('data_process').info(f'Success load user_data of {user_path}!')
            return users

        if self.mode == 'local':
            logging.getLogger('data_process').info(f'Starts to process local user data!')
            users = self.load_local_user()
            logging.getLogger('data_process').info(f'Succeed to process local user data!')
        else:
            logging.getLogger('data_process').info(f'Starts to process hive user data!')
            users = self.load_hive_user()
            logging.getLogger('data_process').info(f'Succeed to process hive user data!')

        logging.getLogger('data_process').info(f'Get {len(users)} users!')

        with open(user_path, "wb") as f:
            pickle.dump(users, f)
            logging.getLogger('data_process').info(f"Succeed save user_data to {user_path}")
        return users

    def get_call(self):
        call_path = self.model_path + f"call_{self.province}_{self.monthid}_{self.mode}.pkl"
        if os.path.exists(call_path):
            with open(call_path, "rb") as f:
                calls = pickle.load(f)

            logging.getLogger('data_process').info(f'Success load call_data of {call_path}!')
            return calls

        if self.mode == 'local':
            logging.getLogger('data_process').info(f'Starts to process local call data!')
            calls = self.load_local_call()
            logging.getLogger('data_process').info(f'Succeed to process local call data!')
        else:
            logging.getLogger('data_process').info(f'Starts to process hive call data!')
            calls = self.load_hive_call()
            logging.getLogger('data_process').info(f'Succeed to process hive call data!')

        logging.getLogger('data_process').info(f'Get {len(calls)} calls!')

        with open(call_path, "wb") as f:
            pickle.dump(calls, f)
            logging.getLogger('data_process').info(f"Succeed save user_data to {call_path}")

        return calls

    def get_tv_user_feature(self):
        if self.mode != 'local':
            logging.getLogger('data_process').info(f'Starts to process hive tv data!')
            values = self.load_hive_tv_user_feature()
            logging.getLogger('data_process').info(f'Succeed to process hive tv data!')
        else:
            logging.getLogger('data_process').info(f'Starts to process local tv data!')
            values = self.load_local_tv_user_feature()
            logging.getLogger('data_process').info(f'Succeed to process local tv data!')

        return values


