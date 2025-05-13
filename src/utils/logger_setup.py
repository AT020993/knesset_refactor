import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR_DEFAULT = Path("logs")
LOG_FILE_MAX_BYTES_DEFAULT = 10 * 1024 * 1024  # 10 MB
LOG_FILE_BACKUP_COUNT_DEFAULT = 5

def setup_logging(
    logger_name: str,
    log_level: int = logging.INFO,
    log_dir: Path = LOG_DIR_DEFAULT,
    log_file_max_bytes: int = LOG_FILE_MAX_BYTES_DEFAULT,
    log_file_backup_count: int = LOG_FILE_BACKUP_COUNT_DEFAULT,
    console_output: bool = True
):
    """
    Configures and returns a logger instance.

    Args:
        logger_name: The name for the logger (e.g., __name__ or a custom name).
        log_level: The minimum log level to capture (e.g., logging.INFO, logging.DEBUG).
        log_dir: The directory to store log files.
        log_file_max_bytes: Maximum size of a log file before rotation.
        log_file_backup_count: Number of backup log files to keep.
        console_output: Whether to output logs to the console.

    Returns:
        A configured logger instance.
    """
    log_dir.mkdir(exist_ok=True)
    logger = logging.getLogger(logger_name)

    # Prevent multiple handlers if logger is already configured (e.g., in Streamlit reruns)
    if logger.hasHandlers():
        logger.setLevel(log_level)
        return logger

    logger.setLevel(log_level)

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s"
    )

    # Console Handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # File Handler (Rotating)
    sanitized_logger_name = "".join(c if c.isalnum() or c in ['_', '-'] else '_' for c in logger_name)
    log_file_path = log_dir / f"{sanitized_logger_name}.log"

    file_handler = RotatingFileHandler(
        log_file_path,
        maxBytes=log_file_max_bytes,
        backupCount=log_file_backup_count,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
