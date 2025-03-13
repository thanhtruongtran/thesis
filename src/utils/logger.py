import json
import logging
import os
from logging.handlers import RotatingFileHandler

FORMATTER = logging.Formatter(
    fmt="[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
    datefmt="%m-%d-%Y %H:%M:%S %Z",
)
LOG_FILE = "logging.log"


def get_console_handler():
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler():
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=2000, backupCount=1)
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(get_console_handler())
    return logger


def write_file1(folder_direction, name, file):
    if not os.path.exists(f"data/log_file/{folder_direction}"):
        try:
            os.makedirs(f"data/log_file/{folder_direction}")
        except Exception:
            with open(f"data/log_file/{folder_direction}/{name}.json", "a") as f:
                f.write(json.dumps(file))
                f.write("\n")

    with open(f"data/log_file/{folder_direction}/{name}.json", "a") as f:
        f.write(json.dumps(file))
        f.write("\n")


def write_file(folder_direction, name, file):
    if not os.path.exists(f"data/log_file/{folder_direction}/{name}.json"):
        try:
            os.makedirs(f"data/log_file/{folder_direction}")
            with open(f"data/log_file/{folder_direction}/{name}.json", "w") as f:
                f.write(json.dumps(file))
                f.write("\n")

        except Exception:
            with open(f"data/log_file/{folder_direction}/{name}.json", "w") as f:
                f.write(json.dumps(file))
                f.write("\n")
    else:
        with open(f"data/log_file/{folder_direction}/{name}.json", "r") as f:
            data_lst = json.load(f)

        data_lst.extend(file)

        with open(f"data/log_file/{folder_direction}/{name}.json", "w") as f:
            f.write(json.dumps(data_lst))
            f.write("\n")
