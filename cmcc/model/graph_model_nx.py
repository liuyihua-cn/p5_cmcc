import os
import sys
import pickle
import logging
import networkx as nx
from pyspark import *
from pyspark.sql import *
# from graphframes import *  # Not needed for NetworkX model
from pyspark.sql import functions
from pyspark.sql.types import *
from data_process.result_process import csv_save


class PersonGraph:
    def __init__(self, config, nodes, edges):
        self.config = config
        self.mode = config['mode']
        self.load_graph_model = config['load_graph_model']
        self.model_path = self.config['output']['local_graph_model_save_path'] if self.mode == 'local' else self.config['output']['graph_model_save_path']
        self.graph_result_table_name = self.config['output']['graph_result_table_name']
        self.node_property = self.config['data_process']['user_features']
        self.edge_property = self.config['data_process']['call_features']
        if self.load_graph_model == "1":
            self.G = self.model_load()
        else:
            self.G = self.create_graph(nodes, edges)
            self.model_save()

        self.init_spark()
        self.inter_dir = self.config['output']['local_inter_save_dir'] if self.mode == 'local' else self.config['output']['inter_save_dir']
        self.province = self.config['province']
        self.statis_ym = self.config['monthid']
        self.node_list = [node for node in list(self.G.nodes(data=True)) if len(node[1]) == len(self.node_property)]
        self.new_rcn = [node for node in self.node_list if len(node[1]) == len(self.node_property) and node[1]['NEW_RCN_ID'] == '1']

        self.msisdn_user_map_path = self.config['output']['msisdn_user_map_path']
        self.load_msisdn_user_map()

    def init_spark(self):
        jar_files = self.config['model']['jar_files']
        conf = SparkConf()
        # origin: 500g/500g
        conf.set('spark.executor.memory', '4g')
        conf.set('spark.driver.memory', '4g')
        conf.setMaster('local')
        conf.set("spark.scheduler.capacity", "10")
        if self.config['mode'] != 'local':
            conf.set("spark.jars", jar_files)
        self.spark = SparkSession.builder.config(conf=conf).appName('graph_model_nx').getOrCreate()

    def load_msisdn_user_map(self):
        # origin: map_path = self.inter_dir + self.msisdn_user_map_path
        map_path = self.model_path + self.msisdn_user_map_path
        if not os.path.exists(map_path):
            logging.getLogger('graph_model').error(map_path + " file not exits!")
            sys.exit(-1)
        else:
            with open(map_path, "rb") as f:
                msisdn_map = pickle.load(f)
            self.msisdn_map = [[k, v] for k, v in msisdn_map.items()]
            logging.getLogger('graph_model').info(f'Success load msisdn_user_map {map_path}')

    def create_graph(self, nodes, edges):
        node_property = self.node_property
        edge_property = self.edge_property

        G = nx.Graph()

        # 修改nodes形式
        nodes_new = []
        node_ids = []
        logging.getLogger('graph_model').info('start create graph_nodes')
        for x in nodes:
            pro = dict(zip(node_property, x))
            nodes_new.append(tuple([x[0], pro]))  # 不能直接改node_id，因为calls中使用的是msisdn
            node_ids.append(x[0])
        G.add_nodes_from(nodes_new)
        del nodes
        logging.getLogger('graph_model').info(f'success create graph_nodes {len(G.nodes)}')

        logging.getLogger('graph_model').info('start create graph_edges')
        if len(edge_property) > 2:
            edges_new = []
            for y in edges:
                pro = dict(zip(edge_property[2:], y[2:]))
                edges_new.append(tuple(y[0:2])+tuple([pro]))

            G.add_edges_from(edges_new)
        else:
            G.add_edges_from(edges)
        del edges
        logging.getLogger('graph_model').info(f'success create graph_edges {len(G.edges)}')

        logging.getLogger('graph_model').info(f'start filter undefined nodes')
        #G = G.subgraph(node_ids)
        sub_Graph = G.subgraph(n for n in G.nodes if G.degree(n) != 0)
        logging.getLogger('graph_model').info(f'success create sub_graph, {len(sub_Graph.nodes)} nodes, {len(sub_Graph.edges)} edges')

        return sub_Graph

    def get_1hop_neighbor(self):
        res = {}
        for n, _ in self.new_rcn:
            try:
                nodes = sorted(nx.neighbors(self.G, n))
                if len(nodes) > 0:
                    res[n] = len(nodes)
            except Exception as e:
                logging.getLogger('graph_model').error(f'Failed get 1-HOP_NEI_COUNT of {n}! {e}')
                continue

        new_clos = ['MSISDN', '1_HOP_NEI_COUNT']
        if len(res) > 0:
            results = [[k, str(v)] for k, v in res.items()]
            results = self.spark.createDataFrame(results, new_clos)
        else:

            schema = StructType(
                [
                    StructField(new_clos[0], StringType(), True),
                    StructField(new_clos[1], StringType(), True)
                ]
            )

            results = self.spark.createDataFrame([], schema)

        result_table_name = f'{new_clos[-1]}_{self.province}_{self.statis_ym}.csv'
        save_path = self.inter_dir + result_table_name

        csv_save(results, save_path)
        logging.getLogger('graph_model').info(f'Results saved to {save_path} successfully!')
        return results

    def get_call_another_user(self):

        res = {}
        for n, value in self.new_rcn:
            try:
                IDTY_NBR = value['IDTY_NBR']
                other_rcn = [node[0] for node in self.node_list if node[1]['IDTY_NBR'] == IDTY_NBR and node[0] != n]
                value = 0
                for r in other_rcn:
                    if self.G.has_edge(n, r):
                        value += 1
                if value > 0:
                    res[n] = value

            except Exception as e:
                logging.getLogger('graph_model').error(f'Failed get CALL_OTHER_USER_COUNT of {n}! {e}')
                continue
        new_clos = ['MSISDN', 'CALL_OTHER_USER_COUNT']
        if len(res) > 0:
            results = [[k, str(v)] for k, v in res.items()]
            results = self.spark.createDataFrame(results, new_clos)
        else:
            schema = StructType(
                [
                    StructField(new_clos[0], StringType(), True),
                    StructField(new_clos[1], StringType(), True)
                ]
            )

            results = self.spark.createDataFrame([], schema)

        result_table_name = f'{new_clos[-1]}_{self.province}_{self.statis_ym}.csv'
        save_path = self.inter_dir + result_table_name

        csv_save(results, save_path)
        logging.getLogger('graph_model').info(f'Results saved to {save_path} successfully!')
        return results

    def get_1hop_connected_neighbor(self):
        res = {}

        for n, _ in self.new_rcn:
            try:
                con = set()
                nodes = nx.neighbors(self.G, n)
                for _n in nodes:
                    _nodes = sorted(nx.common_neighbors(self.G, n, _n))
                    if len(_nodes) > 0:
                        con.add(_n)
                        con.update(set(_nodes))

                if len(con) > 0:
                    res[n] = len(con)

            except Exception as e:
                logging.getLogger('graph_model').error(f'Failed get 1-HOP_CONNECT_NEI_COUNT of {n}! {e}')
                continue

        new_clos = ['MSISDN', "1_HOP_CONNECT_NEI_COUNT"]
        if len(res) > 0:
            results = [[k, str(v)] for k, v in res.items()]
            results = self.spark.createDataFrame(results, new_clos)
        else:
            schema = StructType(
                [
                    StructField(new_clos[0], StringType(), True),
                    StructField(new_clos[1], StringType(), True)
                ]
            )

            results = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), schema)

        result_table_name = f'{new_clos[-1]}_{self.province}_{self.statis_ym}.csv'
        save_path = self.inter_dir + result_table_name

        csv_save(results, save_path)
        logging.getLogger('graph_model').info(f'Results saved to {save_path} successfully!')
        return results

    def get_common_neighbor_with_other_user(self):

        res = {}
        for n, value in self.new_rcn:
            try:
                IDTY_NBR = value['IDTY_NBR']
                old_rcn = [node[0] for node in self.node_list if node[1]['IDTY_NBR'] == IDTY_NBR and node[1]['NEW_RCN_ID'] == '0']
                if len(old_rcn) > 0:
                    con = set()
                    for o in old_rcn:
                        _con = sorted(nx.common_neighbors(self.G, n, o))
                        con.update(set(_con))
                    res[n] = len(con)

            except Exception as e:
                logging.getLogger('graph_model').error(f'Failed get USERS_COMMON_NEI_COUNTUSERS_COMMON_NEI_COUNT of {n}! {e}')
                continue
        new_clos = ['MSISDN', 'USERS_COMMON_NEI_COUNT']
        if len(res) > 0:
            results = [[k, str(v)] for k, v in res.items()]
            results = self.spark.createDataFrame(results, new_clos)
        else:
            schema = StructType(
                [
                    StructField(new_clos[0], StringType(), True),
                    StructField(new_clos[1], StringType(), True)
                ]
            )

            results = self.spark.createDataFrame([], schema)

        result_table_name = f'{new_clos[-1]}_{self.province}_{self.statis_ym}.csv'
        save_path = self.inter_dir + result_table_name

        csv_save(results, save_path)
        logging.getLogger('graph_model').info(f'Results saved to {save_path} successfully!')
        return results

    def get_1hop_neighbor_connected_with_other_user(self):
        res = {}
        for n, value in self.new_rcn:
            try:
                con = set()
                IDTY_NBR = value['IDTY_NBR']
                old_rcn = [node[0] for node in self.node_list if
                           node[1]['IDTY_NBR'] == IDTY_NBR and node[0] != n]
                hop_new = sorted(nx.neighbors(self.G, n))
                for o in old_rcn:
                    hop_old = sorted(nx.neighbors(self.G, o))
                    comm = set(hop_new) & set(hop_old)
                    con.update(comm)

                res[n] = len(con)

            except Exception as e:
                logging.getLogger('graph_model').error(f'Failed get USERS_1-HOP_NEI_CONNECT_COUNT of {n}! {e}')
                continue

        new_clos = ['MSISDN', "USERS_1_HOP_NEI_CONNECT_COUNT"]
        if len(res) > 0:
            results = [[k, str(v)] for k, v in res.items()]
            results = self.spark.createDataFrame(results, new_clos)
        else:
            schema = StructType(
                [
                    StructField(new_clos[0], StringType(), True),
                    StructField(new_clos[1], StringType(), True)
                ]
            )
            results = self.spark.createDataFrame([], schema)

        result_table_name = f'{new_clos[-1]}_{self.province}_{self.statis_ym}.csv'
        save_path = self.inter_dir + result_table_name

        csv_save(results, save_path)
        logging.getLogger('graph_model').info(f'Results saved to {save_path} successfully!')
        return results

    def calculate(self):
        logging.getLogger('graph_model').info('Start Graph calculation!')
        res1 = self.get_1hop_neighbor()
        logging.getLogger('graph_model').info(f'Get 1hop_neighbor! length {res1.count()}, {res1.head()}')

        res2 = self.get_call_another_user()
        logging.getLogger('graph_model').info(f'Get call_another_user! length {res2.count()}, {res2.head()}')

        res = res1.join(res2, 'MSISDN', "outer")
        del res1, res2

        res3 = self.get_1hop_connected_neighbor()
        logging.getLogger('graph_model').info(f'Get 1hop_connected_neighbor! length {res3.count()}, {res3.head()}')

        res = res.join(res3, 'MSISDN', "outer")
        del res3

        res4 = self.get_common_neighbor_with_other_user()
        logging.getLogger('graph_model').info(
            f'Get common_neighbor_with_other_user! length {res4.count()}, {res4.head()}')

        res = res.join(res4, 'MSISDN', "outer")
        del res4

        res5 = self.get_1hop_neighbor_connected_with_other_user()
        logging.getLogger('graph_model').info(
            f'Get 1hop_neighbor_connected_with_other_user! length {res5.count()}, {res5.head()}')

        res = res.join(res5, 'MSISDN', "outer")
        del res5

        res = res.fillna('0')
        # 结果所属数据账期
        res = res.withColumn("STATIS_YM", functions.lit(self.statis_ym))

        logging.getLogger('graph_model').info(f'Finished Graph calculation!')
        logging.getLogger('graph_model').info(f"Get {res.count()} results, {res.head()}")

        # merge user_id
        logging.getLogger('graph_model').info(f'Start to merge MSISDN to USER_ID!')
        schema = StructType(
            [
                StructField("MSISDN", StringType(), True),
                StructField("USER_ID", StringType(), True)
            ]
        )

        df_map = self.spark.createDataFrame(self.msisdn_map, schema=schema)
        df = df_map.join(res, 'MSISDN', 'right')
        logging.getLogger('graph_model').info(f'Merge MSISDN to USER_ID! Get {df.count()} results, {df.head()}')
        df = df.dropna()
        df = df.drop_duplicates(subset=['MSISDN', 'USER_ID'])
        df = df.drop('MSISDN')
        logging.getLogger('graph_model').info(f'Success merge MSISDN to USER_ID! Get final {df.count()} results, {df.head()}')

        result_table_name = self.graph_result_table_name # f'graph_result_{self.province}_{self.statis_ym}.csv'
        save_path = self.inter_dir + result_table_name

        csv_save(df, save_path)
        # csv_save(res, save_path)
        logging.getLogger('graph_model').info(f'Results saved to {save_path} successfully!')

        return df

    def model_save(self):
        model_name = self.config['output']['nx_graph_model_name']
        try:
            with open(self.model_path + model_name, "wb") as f:
                pickle.dump(self.G, f)

            logging.getLogger('graph_model').info(f'Graph_model saved to {self.model_path} successfully!')
        except Exception as e:
            logging.getLogger('graph_model').info(f'Failed to save Graph_model to {self.model_path}! Error: {e}')

    def model_load(self):
        model_name = self.config['output']['nx_graph_model_name']
        if not os.path.exists(self.model_path + model_name):
            logging.getLogger('graph_model').error(self.model_path + model_name + " file not exits!")
            sys.exit(-1)
        else:
            with open(self.model_path + model_name, "rb") as f:
                model = pickle.load(f)
        logging.getLogger('graph_model').info(f'Graph_model load {self.model_path}{model_name} successfully!')
        return model
