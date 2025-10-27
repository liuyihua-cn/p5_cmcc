import os
import yaml
import sys
import time
import datetime
import logging


def load_yamlconf(yaml_root):
    if not os.path.exists(yaml_root):
        logging.getLogger('detection.job').error(yaml_root + " file not exits!")
        sys.exit(-1)
    else:
        with open(yaml_root, "r", encoding="utf-8") as f:
            yaml_conf = yaml.load(f, Loader=yaml.FullLoader)
        return yaml_conf


# 配置文件通配符替换
def yaml_conf_replace(config):
    province = config["province"]
    monthid = config["monthid"]
    need_rep_word = ["${province}", "${monthid}"]
    for path_name in ["data_process", "output"]:
        for name, path in config[path_name].items():
            if isinstance(path, str):
                config[path_name][name] = path.replace(need_rep_word[0], province).replace(need_rep_word[1], monthid)
    return config


def timeStampToDt(time_stamp, fmt="%Y-%m-%d %H:%M:%S"):
    dt = datetime.datetime.fromtimestamp(time_stamp)
    return dt.strftime(fmt)