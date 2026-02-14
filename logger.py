import logging
import sys

def get_logger():
    logger = logging.getLogger('NFTMonitor')
    logger.setLevel(logging.INFO)
    
    file_handler = logging.FileHandler('monitor.log', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S'))
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

setup_logger = get_logger