import logging
from datetime import datetime
import os
import pandas as pd
from finance_complaint.constant import TIMESTAMP
import shutil

LOG_DIR = "logs"


def get_log_file_name():
    return f"log_{TIMESTAMP}.log"


LOG_FILE_NAME = get_log_file_name()

if os.path.exists(LOG_DIR):
    shutil.rmtree(LOG_DIR)
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE_NAME)

logging.basicConfig(filename=LOG_FILE_PATH,
                    filemode="w",
                    format='[%(asctime)s] \t%(levelname)s \t%(lineno)d \t%(filename)s \t%(funcName)s() \t%(message)s',
                    level=logging.DEBUG  # Set logging level to DEBUG
                    )

logger = logging.getLogger("FinanceComplaint")

# Log an info-level message
logger.info("This is an info message")

# Log a debug-level message
logger.debug("This is a debug message")




# import logging
# from datetime import datetime
# import os
# import pandas as pd
# from finance_complaint.constant import TIMESTAMP
# import shutil
# LOG_DIR = "logs"


# def get_log_file_name():
#     return f"log_{TIMESTAMP}.log"


# LOG_FILE_NAME = get_log_file_name()

# if os.path.exists(LOG_DIR):
#     shutil.rmtree(LOG_DIR)
# os.makedirs(LOG_DIR, exist_ok=True)

# LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE_NAME)

# logging.basicConfig(filename=LOG_FILE_PATH,
#                     filemode="w",
#                     format='[%(asctime)s] \t%(levelname)s \t%(lineno)d \t%(filename)s \t%(funcName)s() \t%(message)s',
#                     level=logging.INFO
#                     )

# logger = logging.getLogger("FinanceComplaint")
