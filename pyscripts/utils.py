
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
            logger = logging.getLogger(func.__name__) # Better to name the logger by function
            logger.setLevel(self.level)
            
            # Use delay=True to avoid creating empty files if not needed
            handler = logging.FileHandler(self.log_file)
            formatter = logging.Formatter('%(levelname)s %(asctime)s %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)

            tic = time.time()
            result = func(*args, **kwargs)
            toc = time.time() - tic

            # --- Extraction Logic ---
            # 1. Try to get from kwargs
            year = kwargs.get('year')
            month = kwargs.get('month')

            # 2. Fallback: If not in kwargs, get from positional args 
            # Based on your function: (ds_model, ds_sfc, vars, rename, config, year, month, ...)
            # year is index 5, month is index 6
            if year is None and len(args) > 5:
                year = args[5]
            if month is None and len(args) > 6:
                month = args[6]

            compiling_time = str(datetime.timedelta(seconds=toc))
            
            # Clean log message
            logger.info(f"Task: {func.__name__} | Year: {year} | Month: {month} | Duration: {compiling_time}")

            logger.removeHandler(handler)
            handler.close() # Important to close the file handle
            
            return result
        
        return wrapper