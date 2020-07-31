import logging
import logging.handlers
from datetime import datetime


levels = {"DEBUG": logging.DEBUG,
          "INFO": logging.INFO,
          "ERROR": logging.ERROR,
          "WARNING": logging.WARNING,
          "CRITICAL": logging.CRITICAL
          }

FORMAT = '%(asctime)s %(levelname)s %(name)s %(filename)s %(funcName)s  %(message)s'
def get_logger(name):
    logger = logging.getLogger()  # get root logger
    if logger.handlers:
        return logger
    log_location = "/tmp/"
    current_time = datetime.now()
    current_date = current_time.strftime("%Y-%m-%d")
    file_name = "IRPV_" + current_date + '.log'
    file_location = log_location + file_name
    # To store in file
    logging.basicConfig(format=FORMAT, filemode='a+',
                        filename=file_location,
                        datefmt='%y/%m/%d %T'
                        )
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter(FORMAT)

    stream_handler.setFormatter(formatter)
    stream_handler.setLevel("WARNING")
    logger = logging.getLogger(name)
    logger.setLevel("WARNING")
    logger.addHandler(stream_handler)
    return logger
