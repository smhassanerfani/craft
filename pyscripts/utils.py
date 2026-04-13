
import time
import datetime
import logging
from functools import wraps

class LoggerDecorator(object):
    def __init__(self, log_file, level=logging.INFO):
        self.log_file = log_file
        self.level = level

    def __call__(self, func):
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            
            logger = logging.getLogger()
            logger.setLevel(self.level)
            handler = logging.FileHandler(self.log_file)
            formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)

            tic = time.time()
            result = func(*args, **kwargs)
            toc = time.time() - tic

            # Convert time difference to conventional format
            compiling_time = str(datetime.timedelta(seconds=toc))
            logger.info(f"Run configuration: {args, kwargs}, Compiling time: {compiling_time}")

            logger.removeHandler(handler)
            
            return result
        
        return wrapper