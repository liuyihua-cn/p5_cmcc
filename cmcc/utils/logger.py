# coding = utf-8
import logging
from logging.handlers import RotatingFileHandler


def set_logger(log_file):
    log_fmt = '%(asctime)s %(name)s: %(filename)s[line:%(lineno)d] %(levelname)s: %(message)s'
    formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
    log_file_handler = RotatingFileHandler(filename=log_file,
                                           maxBytes=32 * 1024 * 1024,
                                           backupCount=32,
                                           encoding='utf8')
    log_file_handler.setFormatter(formatter)
    logger = logging.getLogger(name='detection.job')
    logger.addHandler(log_file_handler)
    logger.setLevel(logging.INFO)
    logger = logging.getLogger(name='data_process')
    logger.addHandler(log_file_handler)
    logger.setLevel(logging.INFO)
    logger = logging.getLogger(name='graph_model')
    logger.addHandler(log_file_handler)
    logger.setLevel(logging.INFO)
    logger = logging.getLogger(name='calculate')
    logger.addHandler(log_file_handler)
    logger.setLevel(logging.INFO)
