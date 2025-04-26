import sys
import logging
import logging.config
import configparser
import psycopg2
import re
from datetime import datetime


def parse_log_line(log_line):
    """ Parse one line of Apache access log """

    # start logger
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("modules")

    pattern = re.compile(
        r'(?P<ip_address>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) '
        r'- - '
        r'\[(?P<request_time>.+)\] '
        r'"(?P<request_method>\w+) (?P<request_path>[^"]*) HTTP/(?P<http_version>\d+\.\d+)" '
        r'(?P<status_code>\d+) '
        r'(?P<response_size>\d+) '
        r'"(?P<referer>[^"]*)" '
        r'"(?P<user_agent>[^"]*)"'
    )

    try:
        match = pattern.match(log_line)
        if match:
            return match.groupdict()

    except re.error as e:
        logger.error(f"parse_log_line, regex error: {e}.")


def parse_log_file(log_file):
    """ Parse Apache access log file """

    # start logger
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("modules")

    parsed_logs = []

    try:
        with open(log_file, 'r') as f:
            for line in f:
                parsed_line = parse_log_line(line.strip())

                if parsed_line:
                    parsed_logs.append(parsed_line)

        return parsed_logs

    except FileNotFoundError:
        logger.error(f"parse_log_file, file not found: {log_file}")
        sys.exit()
    except Exception as e:
        logger.error(f"parse_log_file, exception: {e}")


def get_db_config(db_config='configs\\database.ini', section='postgresql'):
    """ Retrieve db config """

    # start logger
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("modules")

    config = {}
    parser = configparser.ConfigParser()

    try:
        parser.read(db_config)
        params = parser.items(section)

        for param in params:
            config[param[0]] = param[1]

        return config

    except FileNotFoundError:
        logger.error(f"get_db_config, file not found: {db_config}.")
        logger.info("Program failed!\n\n")
        sys.exit()
    except configparser.Error as e:
        logger.error(f"get_db_config, ConfigParser error: {e}")
        logger.info("Program failed!\n\n")
        sys.exit()
    except Exception as e:
        logger.error(f"get_db_config, exception: {e}")
        logger.info("Program failed!\n\n")
        sys.exit()


def connect_pgsql(config):
    """ Connect to PostgreSQL db """

    # start logger
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("modules")

    try:
        with psycopg2.connect(**config) as conn:
            conn.autocommit = True
            return conn
            logger.info("Connected to the PostgreSQL server.")

    except (psycopg2.DatabaseError, Exception) as e:
        logger.error(f"connect_pgsql, exception: {e}")
        logger.info("Program failed!\n\n")
        sys.exit()



def convert_timestamp(time_str):
    """ Convert Apache log time string to YYYY-MM-DD HH:MM:SS """

    # start logger
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("modules")

    try:
        time_str = time_str[:20]
        time_str = datetime.strptime(time_str, "%d/%b/%Y:%H:%M:%S")
        time_str = time_str.strftime("%Y-%m-%d %H:%M:%S")
        return time_str

    except ValueError as e:
        print(f"convert_timestamp, ValueError: {e}")
    except TypeError:
        print(f"convert_timestamp, TypeError: {e}")


def insert_log_data(connection, logs):
    """ call stored log in postgresql to insert data """

    # start logger
    logging.config.fileConfig("configs\\logging.ini")
    logger = logging.getLogger("modules")

    try:
        with connection.cursor() as cursor:
            for log in logs:
                cursor.execute("""
                    CALL insert_log_data (
                    %(ip_address)s,
                    %(request_time)s,
                    %(request_method)s,
                    %(request_path)s,
                    %(http_version)s,
                    %(status_code)s,
                    %(response_size)s,
                    %(referer)s,
                    %(user_agent)s
                );
                """, {**log, 'request_time': convert_timestamp(log['request_time'])})

    except (psycopg2.DatabaseError, Exception) as e:
        logger.error(f"insert_log_data, exception: {e}")
