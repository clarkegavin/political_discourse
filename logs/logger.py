# Logs/logger.py
import logging
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:  # prevent duplicate handlers
        # File handler
        #file_handler = logging.FileHandler(f"{LOG_DIR}/{name}.log")

        file_handler = logging.FileHandler(
            f"{LOG_DIR}/{name}.log",
            encoding="utf-8"
        )
        file_handler.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Formatter with module and function
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(module)s.%(funcName)s] %(message)s"
        )

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        console_handler.stream.reconfigure(encoding="utf-8")
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
