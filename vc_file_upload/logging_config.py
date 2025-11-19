import logging


def get_library_logger() -> logging.Logger:
    """
    Get the main library logger for external applications.
    Users can add their own handlers to this logger.

    :return: The main vc_file_upload logger instance
    :rtype: logging.Logger
    """
    return logging.getLogger("vc_file_upload")
