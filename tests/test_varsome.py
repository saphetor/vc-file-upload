import io
import os.path
from queue import Queue
from unittest.mock import MagicMock, call, mock_open, patch

import pytest
from requests import HTTPError

from vc_file_upload.exception import UploadException
from vc_file_upload.varsome import MAX_SINGLE_UPLOAD_BYTES, VarSomeClinicalFileUploader


@pytest.fixture
def mock_http_session():
    with patch("vc_file_upload.varsome.http_session") as mock_session:
        mock_session.return_value = MagicMock()
        yield mock_session


@pytest.fixture
def success_response_json():
    return {"upload_id": "123"}


@pytest.fixture
def mock_http_session_json_response(mock_http_session, success_response_json):
    mock_session_instance = mock_http_session.return_value
    mock_session_instance.post.return_value.json.return_value = success_response_json
    mock_session_instance.put.return_value.json.return_value = success_response_json
    yield mock_http_session


@pytest.fixture
def mock_http_session_raise_for_status(mock_http_session_json_response):
    mock_session_instance = mock_http_session_json_response.return_value
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = HTTPError("HTTP Error")
    mock_session_instance.post.return_value = mock_response
    mock_session_instance.put.return_value = mock_response
    yield mock_http_session_json_response


@pytest.fixture
def mock_http_session_raise_for_status_on_second_call(
    mock_http_session, success_response_json
):
    mock_session_instance = mock_http_session.return_value
    bad_response = MagicMock()
    bad_response.raise_for_status.side_effect = HTTPError(
        "HTTP Error", response=MagicMock(status_code=416)
    )
    good_response = MagicMock()
    good_response.json.return_value = success_response_json
    mock_session_instance.post.side_effect = [
        good_response,
        bad_response,
        good_response,
    ]
    yield mock_http_session


@pytest.fixture
def patch_open_file():
    with patch("builtins.open", mock_open(read_data="data")) as mock_file:
        yield mock_file


@pytest.fixture
def patch_open_file_raise_io_error():
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.side_effect = IOError("IO Error")
        yield mock_file


@pytest.fixture
def patch_upload_local_file(success_response_json):
    with patch(
        "vc_file_upload.varsome.VarSomeClinicalFileUploader._upload_local_file"
    ) as mock_upload_local_file:
        with patch(
            "vc_file_upload.varsome.VarSomeClinicalFileUploader."
            "_upload_local_file_multipart"
        ) as mock_upload_local_file_multi_part:
            mock_upload_local_file.return_value = success_response_json
            mock_upload_local_file_multi_part.return_value = success_response_json
            yield mock_upload_local_file, mock_upload_local_file_multi_part


@pytest.fixture
def patch_upload_chunk(success_response_json):
    with patch(
        "vc_file_upload.varsome.VarSomeClinicalFileUploader._upload_chunk"
    ) as mock_upload_chunk:
        mock_upload_chunk.return_value = success_response_json
        yield mock_upload_chunk


@pytest.fixture
def local_files(tmp_path):
    file_size = 100
    file1 = tmp_path / "file1.txt"
    file1.write_text("A" * file_size)
    nonexistent_file = tmp_path / "nonexistent_file.txt"
    files = {
        str(file1): "file1.txt",
        str(nonexistent_file): "nonexistent_file.txt",
    }
    yield files, file_size


def test_client_context_manager(mock_http_session):
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )
    with client._http_client_session() as session:
        mock_http_session.assert_called_once_with("test_token")
        session.get("http://example.com")
    session.get.assert_called_once_with("http://example.com")
    session.close.assert_called_once()


@pytest.mark.parametrize("base_url", ["http://example.com", "http://example.com/"])
def test_api_url(base_url: str):
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url=base_url
    )
    assert client._api_url("/path/to/resource") == "http://example.com/path/to/resource"


@pytest.mark.usefixtures("mock_http_session_json_response")
def test_retrieve_external_file(success_response_json):
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )
    file_url = "http://server.somewhere.com/file.txt"
    file_name = "file.txt"
    with client._http_client_session() as session:
        response = client._retrieve_external_file(file_url, file_name, session)
    session.post.assert_called_once_with(
        "http://example.com/api/v1/sample-files/",
        json={"file_url": file_url, "sample_file_name": file_name},
    )
    assert response == success_response_json


@pytest.mark.usefixtures("mock_http_session_raise_for_status")
def test_retrieve_external_file_returns_none_on_error():
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )
    file_url = "http://server.somewhere.com/file.txt"
    file_name = "file.txt"
    with client._http_client_session() as session:
        response = client._retrieve_external_file(file_url, file_name, session)
    assert response is None


@pytest.mark.usefixtures("mock_http_session_json_response")
def test_upload_local_file(success_response_json, patch_open_file):
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )

    file_name = "file.vcf"
    file_path = f"/path/to/{file_name}"
    with client._http_client_session() as session:
        response = client._upload_local_file(str(file_path), file_name, session)
    session.put.assert_called_once_with(
        "http://example.com/api/v1/sample-files/upload/",
        data=patch_open_file(),
        headers={"Content-Disposition": f'attachment; filename="{file_name}"'},
    )
    assert response == success_response_json


@pytest.mark.usefixtures("mock_http_session_raise_for_status", "patch_open_file")
def test_upload_local_file_returns_none_on_error():
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )
    file_name = "file.vcf"
    file_path = f"/path/to/{file_name}"
    with client._http_client_session() as session:
        response = client._upload_local_file(str(file_path), file_name, session)
    assert response is None


@pytest.mark.usefixtures(
    "mock_http_session_json_response", "patch_open_file_raise_io_error"
)
def test_upload_local_file_returns_none_on_io_error():
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )
    file_name = "file.vcf"
    file_path = f"/path/to/{file_name}"
    with client._http_client_session() as session:
        response = client._upload_local_file(str(file_path), file_name, session)
    assert response is None


@pytest.mark.usefixtures("mock_http_session_json_response", "patch_upload_local_file")
def test_upload_local_files(local_files, success_response_json):
    files, _ = local_files
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )
    response = client.upload_local_files(files)
    first_file_path = list(files.keys())[0]
    second_file_path = list(files.keys())[1]
    assert response == {
        first_file_path: success_response_json,
        second_file_path: None,
    }


@pytest.mark.usefixtures("mock_http_session_json_response")
def test_retrieve_external_files(success_response_json):
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )
    files = {
        "http://server.somewhere.com/file1.txt": "file1",
        "http://server.somewhere.com/file2.txt": "file2",
    }
    with client._http_client_session() as session:
        result = client.retrieve_external_files(files)
    session.post.assert_has_calls(
        [
            call(
                "http://example.com/api/v1/sample-files/",
                json={
                    "file_url": "http://server.somewhere.com/file1.txt",
                    "sample_file_name": "file1",
                },
            ),
            call(
                "http://example.com/api/v1/sample-files/",
                json={
                    "file_url": "http://server.somewhere.com/file2.txt",
                    "sample_file_name": "file2",
                },
            ),
        ],
        any_order=True,
    )
    assert result == {
        "http://server.somewhere.com/file1.txt": success_response_json,
        "http://server.somewhere.com/file2.txt": success_response_json,
    }


def test_files_with_sizes(local_files):
    files, file_size = local_files
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )
    result = list(client._files_with_sizes(files))
    first_file_path = list(files.keys())[0]
    second_file_path = list(files.keys())[1]
    assert result == [
        (str(first_file_path), os.path.basename(first_file_path), file_size),
        (second_file_path, os.path.basename(second_file_path), None),
    ]


@pytest.mark.usefixtures("mock_http_session")
def test_upload_file_with_strategy(patch_upload_local_file):
    mock_upload_local_file, mock_upload_local_file_multi_part = patch_upload_local_file
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )
    client._multi_part_upload_enabled = True
    with client._http_client_session() as session:
        assert (
            client._upload_file_with_strategy("file.vcf", "file.vcf", None, session)
            is None
        )
        client._upload_file_with_strategy(
            "file.vcf", "file.vcf", MAX_SINGLE_UPLOAD_BYTES - 1, session
        )
        assert mock_upload_local_file.call_count == 1
        assert mock_upload_local_file_multi_part.call_count == 0
        client._upload_file_with_strategy(
            "large_file.vcf",
            "large_file.vcf",
            MAX_SINGLE_UPLOAD_BYTES + 1,
            session,
        )
        assert mock_upload_local_file.call_count == 1
        assert mock_upload_local_file_multi_part.call_count == 1


def test_calculate_file_md5(tmp_path):
    md5_sum_expected = "bea8252ff4e80f41719ea13cdf007273"
    file_path = tmp_path / "test.txt"
    file_path.write_text("Hello, World!\n")

    queue = Queue()
    VarSomeClinicalFileUploader._calculate_file_md5(str(file_path), queue)
    md5_sum_result = queue.get()
    assert md5_sum_result == md5_sum_expected


def test_calculate_file_md5_error(tmp_path):
    file_path = tmp_path / "test.txt"
    queue = Queue()
    VarSomeClinicalFileUploader._calculate_file_md5(str(file_path), queue)
    md5_sum_result = queue.get()
    assert isinstance(md5_sum_result, Exception)


@pytest.mark.usefixtures("mock_http_session_json_response")
def test_complete_multipart_upload(success_response_json):
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )
    with client._http_client_session() as session:
        result = client._complete_multipart_upload("123", "somehash", session)
    session.post.assert_called_once_with(
        "http://example.com/sample-files/filestore-upload/complete/",
        data={"upload_id": "123", "md5": "somehash"},
    )
    assert result == success_response_json


@pytest.mark.usefixtures("mock_http_session_raise_for_status")
def test_complete_multipart_upload_error():
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token",
    )
    with client._http_client_session() as session:
        result = client._complete_multipart_upload("123", "somehash", session)
    assert result is None


@pytest.mark.parametrize(
    "existing_upload_id, expected_params", [(None, {}), ("456", {"upload_id": "456"})]
)
@pytest.mark.usefixtures("mock_http_session_json_response")
def test_upload_chunk_success(existing_upload_id, expected_params):
    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token", clinical_base_url="http://example.com"
    )
    with patch("io.BytesIO", return_value=io.BytesIO(b"0123456789")) as mock_bytes_io:
        with (
            client._http_client_session() as session
        ):  # just to illustrate context manager usage
            upload_id = client._upload_chunk(
                file_name="file.vcf",
                chunk=b"0123456789",
                client=session,
                file_range="0-10/10",
                upload_id=existing_upload_id,
            )
        assert upload_id == "123"
        session.post.assert_called_once_with(
            "http://example.com/sample-files/filestore-upload/add/",
            headers={"Content-Range": "bytes 0-10/10"},
            files={
                "data-file": ("file.vcf", mock_bytes_io(), "application/octet-stream")
            },
            params=expected_params,
        )


@pytest.mark.usefixtures("mock_http_session_raise_for_status")
def test_upload_chunk_raises_upload_exception_on_http_error():
    client = VarSomeClinicalFileUploader(clinical_api_token="t")

    with client._http_client_session() as session:
        with pytest.raises(UploadException):
            client._upload_chunk("f.vcf", b"x", session, "0-1/1")


@pytest.mark.usefixtures("mock_http_session_raise_for_status_on_second_call")
def test_process_multi_part_upload(tmp_path):
    p = tmp_path / "file.bin"
    p.write_bytes(b"0123456789")

    client = VarSomeClinicalFileUploader(
        clinical_api_token="test_token",
        clinical_base_url="http://example.com",
        multipart_upload_chunk_size=6,
    )
    with patch("io.BytesIO", return_value=io.BytesIO(b"0123456789")) as mock_bytes_io:
        with client._http_client_session() as session:
            upload_id = client._process_multi_part_upload(
                str(p), "file.bin", 10, session
            )
        assert upload_id == "123"
        assert session.post.call_count == 3
        session.post.assert_has_calls(
            [
                call(
                    "http://example.com/sample-files/filestore-upload/add/",
                    files={
                        "data-file": (
                            "file.bin",
                            mock_bytes_io(),
                            "application/octet-stream",
                        )
                    },
                    headers={"Content-Range": "bytes 0-6/10"},
                    params={},
                ),
                call(
                    "http://example.com/sample-files/filestore-upload/add/",
                    files={
                        "data-file": (
                            "file.bin",
                            mock_bytes_io(),
                            "application/octet-stream",
                        )
                    },
                    headers={"Content-Range": "bytes 6-10/10"},
                    params={"upload_id": "123"},
                ),
                call(
                    "http://example.com/sample-files/filestore-upload/add/",
                    files={
                        "data-file": (
                            "file.bin",
                            mock_bytes_io(),
                            "application/octet-stream",
                        )
                    },
                    headers={"Content-Range": "bytes 6-10/10"},
                    params={"upload_id": "123"},
                ),
            ]
        )


def test_process_multi_part_upload_returns_none_on_non_416_error(tmp_path):
    p = tmp_path / "f.bin"
    p.write_bytes(b"abcdef")
    client = VarSomeClinicalFileUploader(clinical_api_token="t")

    def side_effect(*args, **kwargs):
        e = UploadException("fail")
        e.status_code = 500
        raise e

    with patch.object(
        VarSomeClinicalFileUploader, "_upload_chunk", side_effect=side_effect
    ):
        result = client._process_multi_part_upload(str(p), "f.bin", 6, MagicMock())
        assert result is None


@pytest.mark.usefixtures(
    "mock_http_session_json_response", "patch_open_file_raise_io_error"
)
def test_process_multi_part_upload_returns_none_on_io_error():
    client = VarSomeClinicalFileUploader(clinical_api_token="t")
    # Force open to raise
    with client._http_client_session() as session:
        result = client._process_multi_part_upload("/no/file", "f.bin", 10, session)
        assert result is None
