import os
import logging
import logging.config
import modules
from datetime import datetime


def main():
 
    # make logs folder if not exists
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # start logger
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("main")
    logger.info("Program started.")

    curr_dir = os.getcwd()
    root_dir = os.path.dirname(os.path.dirname(curr_dir))
    data_file = root_dir + '\\data\\apache_logs.txt'

    all_logs = modules.parse_log_file(data_file)

    config = modules.get_db_config()
    pgconn = modules.connect_pgsql(config)

    modules.insert_log_data(pgconn, all_logs)

    logger.info("Program done!\n")


if __name__ == "__main__":
    main()
