import os
import warnings
import logging
import argparse
warnings.filterwarnings('ignore')

from data_process.data_process import DataProcessor
from model.model import Model
from data_process.result_process import csv_save
from utils.logger import set_logger
from utils.common import *


parser = argparse.ArgumentParser(description="", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--yaml_root', type=str, default='./config.yaml', help='yaml context root')
parser.add_argument('--province', type=str, default="shandong", help='the province which data belongs to')
parser.add_argument('--monthid', type=str, default="202305", help='the monthid which data belongs')
parser.add_argument('--mode', type=str, default="local", help='the running mode')
parser.add_argument('--model_type', type=str, default="nx", help='the model type')
parser.add_argument('--load_graph_model', type=str, default="0", help='if load pre-model')
parser.add_argument('--only_graph', type=str, default="1", help='only do graph_calculation')
parser.add_argument('--only_tv', type=str, default="0", help='only do tv_calculation')
parser.add_argument('--load_graph_result', type=str, default="0", help='if load pre-graph-result')
parser.add_argument('--load_tv_result', type=str, default="0", help='if load pre-tv-result')

args = parser.parse_args()

set_logger(f'./logs/detection_{args.province}_{args.monthid}.out')


def main():
    config = load_yamlconf(args.yaml_root)

    config['mode'] = args.mode
    logging.getLogger('detection.job').info(f"Running on {args.mode} mode!")

    config['model_type'] = args.model_type
    config['province'] = args.province
    config['monthid'] = args.monthid
    config['load_graph_model'] = args.load_graph_model
    config['load_graph_result'] = args.load_graph_result
    config['load_tv_result'] = args.load_tv_result
    config['only_graph'] = args.only_graph
    config['only_tv'] = args.only_tv

    config = yaml_conf_replace(config)

    logging.getLogger('detection.job').info(f"Get config {config}!")
    if config['mode'] == 'local':
        pyspark_python = config['global']['local_pyspark_python']
        pyspark_driver_python = config['global']['local_pyspark_driver_python']
    else:
        pyspark_python = config['global']['pyspark_python']
        pyspark_driver_python = config['global']['pyspark_driver_python']

    os.environ['PYSPARK_PYTHON'] = pyspark_python
    os.environ['PYSPARK_DRIVER_PYTHON'] = pyspark_driver_python

    model = Model(config)
    res = model.calculate()
    logging.getLogger('graph_model').info(f'Finished model calculation!')
    logging.getLogger('graph_model').info(f"Get {res.count()} results, {res.head()}")

    model.results_save(res)


if __name__ == "__main__":
    logging.getLogger('detection.job').info(f"Start unreal person detecting!")
    try:
        main()
        logging.getLogger('detection.job').info('Success unreal person detection!')
    except Exception as e:
        logging.getLogger('detection.job').error(f'Failed unreal person detection! Error {e}.')