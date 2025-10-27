import logging
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

from data_process.data_process import DataProcessor
from model.graph_model_nx import PersonGraph as GraphNx
# from model.graph_model_gf import PersonGraph as GraphGf  # Only import when needed
from model.user_ori_pref import UserOriFeature
from data_process.result_process import csv_save


class Model:
    def __init__(self, config):
        self.config = config
        self.mode = config['mode']
        self.province = config['province']
        self.statis_ym = config['monthid']
        self.inter_dir = self.config['output']['local_inter_save_dir'] if self.mode == 'local' else \
        self.config['output']['inter_save_dir']
        self.model_type = config['model_type']
        self.load_graph_result = self.config['load_graph_result']
        self.load_tv_result = self.config['load_tv_result']
        self.only_graph = self.config['only_graph']
        self.only_tv = self.config['only_tv']

        self.graph_result_table_name = self.config['output']['graph_result_table_name']
        self.tv_result_table_name = self.config['output']['tv_result_table_name']
        self.final_result_table_name = self.config['output']['result_table_name']
        self.result_save_dir = config['output']['local_result_save_dir'] if config['mode'] == 'local' else \
        config['output']['hdfs_result_save_dir']

        self.nodes = []
        self.edges = []

        self.processor = DataProcessor(config)

        self.init_spark()

    def init_spark(self):
        jar_files = self.config['model']['jar_files']
        conf = SparkConf()
        # origin: 30g/30g
        conf.set('spark.executor.memory', '4g')
        conf.set('spark.driver.memory', '4g')
        conf.setMaster('local')
        conf.set("spark.scheduler.capacity", "10")
        if self.config['mode'] != 'local':
            conf.set("spark.jars", jar_files)
        self.spark = SparkSession.builder.config(conf=conf).appName('model').getOrCreate()

    def calculate(self):
        if self.only_graph == '1':
            res1 = self.graph_calculate()
            logging.getLogger('calculate').info(f"Get Only_graph {res1.count()} results, {res1.head()}")
            return res1

        if self.only_tv == '1':
            res2 = self.tv_calculate()
            logging.getLogger('calculate').info(f"Get Only_tv {res2.count()} results, {res2.head()}")
            return res2

        res1 = self.graph_calculate()
        res2 = self.tv_calculate()

        results = res1.join(res2, 'USER_ID', "right")
        results = results.fillna('null')
        logging.getLogger('calculate').info(f"Get total {results.count()} results, {results.head()}")

        return results

    def results_save(self, res):
        if self.only_graph == '1':
            table_name = self.graph_result_table_name
        elif self.only_tv == '1':
            table_name = self.tv_result_table_name
        else:
            table_name = self.final_result_table_name

        save_path = self.result_save_dir + table_name
        csv_save(res, save_path)
        # 本地备份
        local_path = self.inter_dir + table_name
        csv_save(res, local_path)

    def graph_calculate(self):
        if self.load_graph_result == '1':
            logging.getLogger('calculate').info(f"Loading graph_result...")
            data_path = self.inter_dir + self.graph_result_table_name
            res1 = self.spark.read.csv(data_path, header=True)
            logging.getLogger('calculate').info(f"Succeed to load {res1.count()} graph result, {res1.head()}")
        else:
            load_graph_model = self.config['load_graph_model']
            if load_graph_model == '1':
                nodes = []
                edges = []
            else:
                nodes = self.processor.get_user()
                edges = self.processor.get_call()
                self.nodes = nodes
                self.edges = edges
            if self.model_type == "nx":
                self.G = GraphNx(self.config, nodes, edges)
            else:
                # Import GraphGf only when needed
                from model.graph_model_gf import PersonGraph as GraphGf
                self.G = GraphGf(self.config, nodes, edges)

            res1 = self.G.calculate()

            logging.getLogger('calculate').info(f"Get graph {res1.count()} results, {res1.head()}")

        return res1

    def tv_calculate(self):
        self.tv_user = UserOriFeature(self.config)
        if self.load_tv_result == '1':
            logging.getLogger('calculate').info(f"Loading tv_result...")
            data_path = self.inter_dir + self.tv_result_table_name
            res2 = self.spark.read.csv(data_path, header=True)
            logging.getLogger('calculate').info(f"Succeed to load tv result")
        else:
            logging.getLogger('calculate').info(f"Start tv calculation...")
            if len(self.nodes) <= 0:
                nodes = self.processor.get_user()
            else:
                nodes = self.nodes
            res2 = self.calculate_tv_ori_diff(nodes)

            logging.getLogger('calculate').info(f"Finished tv calculation")
            save_path = self.inter_dir + self.tv_result_table_name  # f'tv_result_{self.province}_{self.statis_ym}.csv'
            csv_save(res2, save_path)

        logging.getLogger('calculate').info(f"Get tv_ori_diff {res2.count()} results, {res2.head()}")
        return res2

    def calculate_tv_ori_diff(self, node):
        group_map = self.get_new_old_group(node)
        user_indi_map = self.tv_user.calculate()
        _res = []
        for idty, _dic in group_map.items():
            new_list = _dic['new']
            old_list = _dic['old']
            if len(new_list) > 0 and len(old_list) > 0:
                wady_onnet_flux = [user_indi_map[k]['wady_onnet_flux_pref'] for k in old_list if k in user_indi_map]
                nwady_onnet_flux = [user_indi_map[k]['nwady_onnet_flux_pref'] for k in old_list if k in user_indi_map]
                flux_fee_pref = [user_indi_map[k]['flux_fee_pref'] for k in old_list if k in user_indi_map]
                pack_mon_pref = [user_indi_map[k]['pack_mon_pref'] for k in old_list if k in user_indi_map]

                logging.getLogger('calculate').info(
                    f"Get {len(wady_onnet_flux)} wady_onnet_flux, {len(nwady_onnet_flux)} nwady_onnet_flux, {len(flux_fee_pref)} flux_fee_pref, {len(pack_mon_pref)} pack_mon_pref.")

                avg_wady_onnet_flux_pref = sum(wady_onnet_flux) / len(wady_onnet_flux) if len(
                    wady_onnet_flux) > 0 else 0
                avg_nwady_onnet_flux_pref = sum(nwady_onnet_flux) / len(nwady_onnet_flux) if len(
                    nwady_onnet_flux) > 0 else 0
                avg_flux_fee_pref = sum(flux_fee_pref) / len(flux_fee_pref) if len(flux_fee_pref) > 0 else 0
                avg_pack_mon_pref = sum(pack_mon_pref) / len(pack_mon_pref) if len(pack_mon_pref) > 0 else 0

                for user in new_list:
                    if user in user_indi_map:
                        wady_onnet_flux_pref_diff = user_indi_map[user][
                                                        'wady_onnet_flux_pref'] - avg_wady_onnet_flux_pref
                        nwady_onnet_flux_pref_diff = user_indi_map[user][
                                                         'nwady_onnet_flux_pref'] - avg_nwady_onnet_flux_pref
                        flux_fee_pref_diff = user_indi_map[user]['flux_fee_pref'] - avg_flux_fee_pref
                        pack_mon_pref_diff = user_indi_map[user]['pack_mon_pref'] - avg_pack_mon_pref
                        _res.append([user, wady_onnet_flux_pref_diff, nwady_onnet_flux_pref_diff, flux_fee_pref_diff,
                                     pack_mon_pref_diff])

        schema = StructType(
            [
                StructField("USER_ID", StringType(), True),  # MSISDN
                StructField("wady_onnet_flux_pref_diff".upper(), StringType(), True),
                StructField("nwady_onnet_flux_pref_diff".upper(), StringType(), True),
                StructField("flux_fee_pref_diff".upper(), StringType(), True),
                StructField("pack_mon_pref_diff".upper(), StringType(), True),
            ]
        )
        res = self.spark.createDataFrame(_res, schema)
        return res

    def get_new_old_group(self, node):
        group_map = {
        }
        for value in node:
            idty = value[4]
            # msisdn = value[0]
            user_id = value[1]
            is_new = value[2]
            if idty not in group_map:
                new_list = []
                old_list = []
                if is_new == '1':
                    new_list.append(user_id)  # msisdn
                else:
                    old_list.append(user_id)
                group_map[idty] = {
                    'new': new_list,
                    'old': old_list
                }
            else:
                if is_new == '1':
                    new_list = group_map[idty]['new']
                    new_list.append(user_id)
                    group_map[idty]['new'] = new_list
                else:
                    old_list = group_map[idty]['old']
                    old_list.append(user_id)
                    group_map[idty]['old'] = old_list

        logging.getLogger('calculate').info(f"Get {len(group_map)} group_map, {group_map}")
        return group_map
