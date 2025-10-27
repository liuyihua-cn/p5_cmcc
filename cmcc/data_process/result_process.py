import logging


def csv_save(results, save_path):
    try:
        results.repartition(1).write.mode('overwrite').option("sep", ",").option("header", True).option("encoding", "utf-8").csv(save_path)
        logging.getLogger('detection.job').info(f'Results saved to {save_path} successfully!')
    except Exception as e:
        logging.getLogger('detection.job').info(f'Failed to saved {save_path}! Error {e}')
