import logging
import os
from pathlib import Path

def _resolve_log_target(caller_file_path: str):
    """
    Determines logging mode (Lambda vs local) and resolves
    log directory and log file name for local development.
    """
    IS_LAMBDA = os.environ.get("AWS_LAMBDA_FUNCTION_NAME") is not None

    if IS_LAMBDA:
        # Then CloudWatch logging
        return True, None, None

    # Local dev logging
    parents = Path(caller_file_path).resolve().parents
    if len(parents) <= 5:
        raise RuntimeError("Cannot resolve project root from caller_file_path")
    
    project_dir = parents[5]
    logs_dir = project_dir / "logs"
    log_file_name = Path(caller_file_path).stem + ".log"

    return IS_LAMBDA, logs_dir, log_file_name


def get_logger(logger_name=__name__, caller_file_path=None):
    """
        Sets up and returns a logger instance
        If running in AWS Lambda, logs to CloudWatch
        If running locally, logs to specified file in log_dir/log_file_name
        Args:
            logger_name (str): Name of the logger
            log_dir (str): Directory to store log files
            log_file (str): Log file name
        Returns:
            logging.Logger: Configured logger instance
    """
    
    IS_LAMBDA, log_dir, log_file_name = _resolve_log_target(caller_file_path)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # If logger already has handlers, return to avoid duplicate logs
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "[%(levelname)s | %(name)s | %(asctime)s.%(msecs)03dZ] - [%(filename)s | %(module)s | %(funcName)s | L%(lineno)d] : %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )

    if IS_LAMBDA:
        stream_handler = logging.StreamHandler() # Log to stdout (CloudWatch)
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    if log_dir and log_file_name:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / log_file_name, mode="a")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger