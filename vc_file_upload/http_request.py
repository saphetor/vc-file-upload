from typing import List

import requests
from requests.adapters import HTTPAdapter, Retry

from vc_file_upload import __version__


class TimeOutSession(requests.Session):

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Override the request method to set a timeout for all requests.
        Sensible timeouts are set to 10 seconds for the connection and
        30 seconds for the read.
        :param method: HTTP method (GET, POST, etc.)
        :param url: URL for the request
        :return: Response object
        """
        timeout = kwargs.get("timeout", (10, 60))
        kwargs.setdefault("timeout", timeout)
        return super().request(method, url, **kwargs)


def http_session(
    token: str,
    retries: int = 5,
    backoff: float = 1.0,
    retry_http_codes: List[int] = None,
) -> requests.Session:
    """
    Creates and configures an HTTP session with retry capabilities
    and bearer token authorization headers.

    :param token: The authorization token to be used for setting the `Authorization`
        header in the session.
    :type token: str
    :param retries: The maximum number of retries allowed for failed HTTP requests.
    :type retries: int
    :param backoff: The backoff factor to apply between retry attempts, allowing
        exponential backoff delays.
    :type backoff: float
    :param retry_http_codes: The list of HTTP status codes that should trigger a retry.
        Defaults to [503, 502, 429] if not specified.
    :type retry_http_codes: List[int]
    :return: A configured `requests.Session` object with custom retry logic and
        authorization headers.
    :rtype: requests.Session
    """
    if retry_http_codes is None:
        retry_http_codes = [503, 502, 429]

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": f"vc-file-upload-client/{__version__}",
    }
    retry_policy = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=retry_http_codes,
        allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"],
    )
    adapter = HTTPAdapter(max_retries=retry_policy)
    session = TimeOutSession()
    session.headers.update(headers)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session
