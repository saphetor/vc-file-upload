from typing import List

import requests
from requests.adapters import HTTPAdapter, Retry

from vc_file_upload import __version__


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
        retry_http_codes = [504, 503, 502, 429]

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "User-Agent": f"vc-file-upload-client/{__version__}",
    }
    retry_policy = Retry(
        total=None,
        status=retries,
        connect=2,
        read=0,
        redirect=0,
        backoff_factor=backoff,
        status_forcelist=retry_http_codes,
        allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"],
        other=0,
    )
    adapter = HTTPAdapter(max_retries=retry_policy)
    session = requests.Session()
    session.headers.update(headers)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session
