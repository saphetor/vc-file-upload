class StorageException(Exception):
    pass


class UnknownStorageException(ValueError):
    pass


class UploadException(Exception):

    status_code = None

    def __init__(self, message, original_exception=None):
        super().__init__(message)
        if http_response := getattr(original_exception, "response", None):
            self.status_code = http_response.status_code
