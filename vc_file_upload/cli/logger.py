import logging

from vc_file_upload.logging_config import get_library_logger


def _get_logger():
    """
    Returns a logger instance for the dx_vc_file_transfer module.
    """
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    logger = get_library_logger()
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


logger = _get_logger()
