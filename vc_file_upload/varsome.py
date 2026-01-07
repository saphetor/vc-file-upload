import contextlib
import dataclasses
import hashlib
import io
import pathlib
import threading
from queue import Queue
from typing import Dict, Iterator, Optional, Tuple
from urllib.parse import urljoin

import requests

from vc_file_upload.exception import UploadException
from vc_file_upload.http_request import http_session
from vc_file_upload.logging_config import get_library_logger

logger = get_library_logger()

DEFAULT_BASE_URL = "https://ch.clinical.varsome.com"
MAX_SINGLE_UPLOAD_BYTES = 100 * 1024 * 1024
MULTIPART_CHUNK_BYTES = 20 * 1024 * 1024


@dataclasses.dataclass(kw_only=True)
class VarSomeClinicalFileUploader:
    """
    Handles interactions with the VarSome Clinical API.
    Provides methods to retrieve external files and upload local files.

    :ivar clinical_api_token: The authentication token for accessing the
        clinical API.
    :type clinical_api_token: str
    :ivar clinical_base_url: The base URL for the clinical API. Defaults to
        "https://ch.clinical.varsome.com".
    :type clinical_base_url: str
    """

    clinical_api_token: str
    clinical_base_url: str = DEFAULT_BASE_URL
    max_single_file_upload_size_bytes: int = MAX_SINGLE_UPLOAD_BYTES
    multipart_upload_chunk_size: int = MULTIPART_CHUNK_BYTES

    @contextlib.contextmanager
    def _http_client_session(self):
        """
        Context manager to create and manage the HTTP client session.
        """
        client = http_session(self.clinical_api_token)
        try:
            yield client
        finally:
            client.close()

    def _api_url(self, url: str) -> str:
        """
        Returns the full URL for a given relative URL.

        :param url: The relative URL to append to the base URL.
        :type url: str
        :return: The full URL with the base URL appended.
        """
        return urljoin(self.clinical_base_url, url)

    def retrieve_external_files(
        self, files: Dict[str, str]
    ) -> Dict[str, Optional[Dict]]:
        """
        Retrieve multiple external files from the clinical API.

        :param files: A dictionary where keys are file URLs and values are file names.
        :type files: Dict[str, str]
        :return: A dictionary containing metadata for each retrieved file (if any).
        """
        with self._http_client_session() as client:
            return {
                file_url: self._retrieve_external_file(file_url, file_name, client)
                for file_url, file_name in files.items()
            }

    def _retrieve_external_file(
        self, file_url: str, file_name: str, client: requests.Session
    ) -> Optional[Dict]:
        """
        Retrieve an external file from the clinical API.

        :param file_url: The URL of the file to retrieve.
        :type file_url: str
        :param file_name: The name of the file to save.
        :type file_name: str
        :param client: The HTTP client session to use for the request.
        :type client: requests.Session
        :return: A dictionary containing the file metadata.
        """
        api_url = self._api_url("/api/v1/sample-files/")
        params = {"file_url": file_url, "sample_file_name": file_name}
        logger.info("Retrieving external file %s from url %s", file_name, file_url)
        try:
            response = client.post(api_url, json=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.exception(
                "Failed to retrieve external file",
                extra={
                    "file_url": file_url,
                    "file_name": file_name,
                    "api_url": api_url,
                    "error": str(e),
                },
            )
        return None

    def upload_local_files(self, files: Dict[str, str]) -> Dict[str, Optional[Dict]]:
        """
        Upload multiple local files to the clinical API.
        Depending on the file size, it chooses between full and multipart upload.
        :param files: A dictionary where keys are file paths and values are file names.
        :type files: Dict[str, str]
        :return: A dictionary containing metadata for each uploaded file.
        """
        with self._http_client_session() as client:
            return {
                file_path: self._upload_file_with_strategy(
                    file_path, file_name, file_size, client
                )
                for file_path, file_name, file_size in self._files_with_sizes(files)
            }

    @staticmethod
    def _files_with_sizes(
        files: Dict[str, str],
    ) -> Iterator[Tuple[str, str, Optional[int]]]:
        """
        Calculate and yield the size of files along with their paths and names.

        :param files: A dictionary where the key is the file path
            as a string and the value
            is the file name as a string.
        :type files: Dict[str, str]

        :return: A generator that yields tuples in the
            format (file_path, file_name, file_size).
        :rtype: Iterator[Tuple[str, str, Optional[int]]]
        """
        for file_path, file_name in files.items():
            try:
                file_size = pathlib.Path(file_path).stat().st_size
                yield file_path, file_name, file_size
            except (IOError, OSError) as e:
                logger.exception(
                    "Failed to calculate file size",
                    extra={
                        "file_path": file_path,
                        "file_name": file_name,
                        "error": str(e),
                    },
                )
                yield file_path, file_name, None

    def _upload_file_with_strategy(
        self,
        file_path: str,
        file_name: str,
        file_size: Optional[int],
        client: requests.Session,
    ) -> Optional[Dict]:
        """
        Uploads a file to a remote server using
        an appropriate strategy based on the file size.

        :param file_path: The local path of the file to be uploaded.
        :param file_name: The name of the file to be stored remotely.
        :param file_size: The size of the file in bytes. If ``None``,
            the upload is skipped.
        :param client: An instance of ``requests.Session`` used for making
            HTTP requests.
        :return: A dictionary containing upload response details if successful;
            ``None`` otherwise.
        """
        if file_size is None:
            return None

        if file_size <= self.max_single_file_upload_size_bytes:
            return self._upload_local_file(file_path, file_name, client)

        return self._upload_local_file_multipart(
            file_path=file_path,
            file_name=file_name,
            file_size=file_size,
            client=client,
        )

    def _upload_local_file(
        self, file_path: str, file_name: str, client: requests.Session
    ) -> Optional[Dict]:
        """
        Uploads a local file to a specified API endpoint using the provided HTTP client.

        :param file_path: The full path to the file to be uploaded.
        :param file_name: The name of the file to attach in the upload request.
        :param client: HTTP session object used to perform the PUT request.
        :type client: requests.Session
        :return: A dictionary containing the response data from the upload API.
        :rtype: Dict
        """
        api_url = self._api_url("/api/v1/sample-files/upload/")
        extra_headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
        logger.info("Uploading local file %s from path %s", file_name, file_path)
        try:
            with open(file_path, "rb") as file:
                response = client.put(api_url, data=file, headers=extra_headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.exception(
                "Failed to upload local file",
                extra={
                    "file_path": file_path,
                    "file_name": file_name,
                    "api_url": api_url,
                    "error": str(e),
                },
            )
        except (IOError, OSError) as e:
            logger.exception(
                "File read error during upload",
                extra={
                    "file_path": file_path,
                    "file_name": file_name,
                    "api_url": api_url,
                    "error": str(e),
                },
            )
        return None

    def _upload_local_file_multipart(
        self, file_path: str, file_name: str, file_size: int, client: requests.Session
    ) -> Optional[Dict]:
        """
        Uploads a file to a remote server using a multipart upload. Calculates the MD5
        checksum in a separate thread while starting the multipart upload process.
        integrity.

        :param file_path: The path to the local file to be uploaded.
        :param file_name: The name to assign to the uploaded file.
        :param file_size: The size of the file in bytes.
        :param client: The HTTP client session to use for the upload.
        :return: A dictionary containing the result of the multipart upload operation.
        :rtype: Dict
        :raises Exception: If the MD5 checksum calculation fails or
            any part of the multipart upload process encounters an error.
        """
        queue = Queue()
        md5_sum_thread = threading.Thread(
            target=self._calculate_file_md5, args=(file_path, queue)
        )
        md5_sum_thread.start()
        upload_id = self._process_multi_part_upload(
            file_path, file_name, file_size, client
        )
        md5_sum_thread.join()
        md5_sum = queue.get()
        if isinstance(md5_sum, Exception):
            logger.exception(
                "Failed to calculate file MD5",
                extra={
                    "file_path": file_path,
                    "file_name": file_name,
                    "error": str(md5_sum),
                },
            )
            return None
        if upload_id is None:
            logger.error(
                "Multipart upload failed: No upload ID returned",
                extra={
                    "file_path": file_path,
                    "file_name": file_name,
                },
            )
            return None
        return self._complete_multipart_upload(upload_id, md5_sum, client)

    def _process_multi_part_upload(
        self, file_path: str, file_name: str, file_size: int, client: requests.Session
    ) -> Optional[str]:
        """
        Processes and uploads a file in chunks using multipart upload. Reads
        the file in specified chunk sizes, uploads each chunk sequentially, and retries
        if specific exceptions are encountered.

        :param file_path: Path to the file being uploaded.
        :type file_path: str
        :param file_name: Name of the file to identify it on the server.
        :type file_name: str
        :param file_size: Size of the file in bytes.
        :type file_size: int
        :param client: HTTP client session to be used for the upload
            (e.g., requests.Session).
        :type client: requests.Session
        :return: Upload ID for the successfully uploaded multipart file.
        :rtype: str
        """
        start = 0
        upload_id = None
        chunk_size = self.multipart_upload_chunk_size
        logger.info(
            "Starting multipart upload for file %s from path %s", file_name, file_path
        )
        try:
            with open(file_path, "rb") as file:
                while True:
                    end = min(start + chunk_size, file_size)
                    file.seek(start)
                    chunk = file.read(chunk_size)
                    try:
                        upload_id = self._upload_chunk(
                            file_name,
                            chunk,
                            client,
                            f"{start}-{end}/{file_size}",
                            upload_id,
                        )
                        if end == file_size:
                            return upload_id
                        start = end
                    except UploadException as e:
                        if e.status_code == 416:
                            response = e.original_exception.response
                            if server_offset := response.json().get("offset"):
                                start = server_offset
                                continue
                        logger.exception(
                            "Failed to upload local file",
                            extra={
                                "file_path": file_path,
                                "file_name": file_name,
                                "error": str(e),
                            },
                        )
                        return None
        except (IOError, OSError) as e:
            logger.exception(
                "File read error during multipart upload",
                extra={
                    "file_path": file_path,
                    "file_name": file_name,
                    "error": str(e),
                },
            )
            return None

    def _upload_chunk(
        self,
        file_name: str,
        chunk: bytes,
        client: requests.Session,
        file_range: str,
        upload_id: Optional[str] = None,
    ) -> str:
        """
        Handles the upload of a specific chunk of a file to the server.

        :param file_name: Name of the file being uploaded.
        :type file_name: str
        :param chunk: The binary data representing the chunk of the file
            to be uploaded.
        :type chunk: bytes
        :param client: An HTTP session client to use for sending requests.
        :type client: requests.Session
        :param file_range: Specifies the byte range of the chunk in the format
            'start-end'.
        :type file_range: str
        :param upload_id: Optional identifier for the ongoing upload process.
        :type upload_id: Optional[str]
        :return: Unique upload ID returned by the server for the current or
            continued upload process.
        :rtype: str
        :raises UploadException: Raised when the upload process fails due to a request
            or response issue, such as network errors or malformed server responses.
        """
        api_url = self._api_url("/sample-files/filestore-upload/add/")
        extra_headers = {
            "Content-Range": f"bytes {file_range}",
        }
        logger.info("Uploading chunk %s for file %s", file_range, file_name)
        try:
            form_data = {
                "data-file": (file_name, io.BytesIO(chunk), "application/octet-stream"),
            }
            request_params = {}
            if upload_id:
                request_params["upload_id"] = upload_id
            response = client.post(
                api_url, files=form_data, headers=extra_headers, params=request_params
            )
            logger.info("Chunk upload response status: %s", response.status_code)
            response.raise_for_status()
            return response.json()["upload_id"]
        except requests.exceptions.RequestException as e:
            raise UploadException(
                f"Failed to upload file {file_name}", original_exception=e
            ) from e
        except KeyError as e:
            raise UploadException(
                f"Invalid response while uploading file {file_name}",
                original_exception=e,
            ) from e

    def _complete_multipart_upload(
        self, upload_id: str, md5_sum, client: requests.Session
    ) -> Optional[Dict]:
        """
        Completes a multipart upload by finalizing the process
        and providing the necessary
        details such as upload ID and MD5 checksum.

        :param upload_id: The unique identifier of the upload session.
        :param md5_sum: The MD5 checksum of the uploaded file for verification.
        :param client: The HTTP session client used to communicate with the API.
        :return: JSON response from the API indicating the result of the upload.
        :rtype: dict
        :raises UploadException: If the upload completion fails due to communication or
                                 other issues.
        """
        api_url = self._api_url("/sample-files/filestore-upload/complete/")
        logger.info("Completing multipart upload with ID %s", upload_id)
        try:
            response = client.post(
                api_url, data={"upload_id": upload_id, "md5": md5_sum}
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.exception(
                "Failed to complete multipart upload",
                extra={
                    "upload_id": upload_id,
                    "md5": md5_sum,
                    "api_url": api_url,
                    "error": str(e),
                },
            )
        return None

    @staticmethod
    def _calculate_file_md5(file_path: str, queue: Queue):
        """
        Calculate the MD5 hash of a file over using a thread
        and places the result into a provided queue.
        :param file_path: The path to the file whose MD5 hash is to be calculated.
        :type file_path: str
        :param queue: A queue where the resulting MD5 hash or
            an exception will be placed.
        :type queue: Queue
        :return: None
        """
        md5_hash = hashlib.md5()
        logger.info("Calculating MD5 hash for file %s", file_path)
        try:
            with open(file_path, "rb") as f:
                while chunk := f.read(8192):
                    md5_hash.update(chunk)
            queue.put(md5_hash.hexdigest())
        except (OSError, IOError) as e:
            queue.put(e)
