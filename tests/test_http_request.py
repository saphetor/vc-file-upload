from unittest.mock import MagicMock, patch

import pytest

from vc_file_upload import __version__
from vc_file_upload.http_request import http_session


@pytest.mark.parametrize(
    "retry_http_codes, expected_codes",
    [
        ([500, 502], [500, 502]),
        (None, [504, 503, 502, 429]),
    ],
)
def test_http_session_configures_headers_and_retries(retry_http_codes, expected_codes):
    token = "test_token"
    with (
        patch("vc_file_upload.http_request.HTTPAdapter") as mock_adapter,
        patch("requests.Session") as mock_session,
        patch("vc_file_upload.http_request.Retry") as mock_retry,
    ):
        mock_client = MagicMock()
        mock_session.return_value = mock_client

        http_session(token, retries=3, backoff=0.5, retry_http_codes=retry_http_codes)

        mock_retry.assert_called_once_with(
            total=None,
            status=3,
            connect=2,
            read=0,
            redirect=0,
            backoff_factor=0.5,
            status_forcelist=expected_codes,
            allowed_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"],
            other=0,
        )
        mock_adapter.assert_called_once_with(max_retries=mock_retry.return_value)
        mock_client.headers.update.assert_called_once_with(
            {
                "Accept": "application/json",
                "Authorization": f"Bearer {token}",
                "User-Agent": f"vc-file-upload-client/{__version__}",
            }
        )
        mock_client.mount.assert_any_call("http://", mock_adapter.return_value)
        mock_client.mount.assert_any_call("https://", mock_adapter.return_value)
