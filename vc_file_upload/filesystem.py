import dataclasses
import pathlib
import sys
from typing import Dict, List, Optional, Set

from vc_file_upload import exception
from vc_file_upload.config import (
    ALLOWED_FILE_EXTENSIONS,
    DEFAULT_STORAGE_BACKEND,
    STORAGE_BACKEND_LOCAL,
    STORAGE_BACKEND_TYPE,
)
from vc_file_upload.logging_config import get_library_logger
from vc_file_upload.storage import create_storage

logger = get_library_logger()


@dataclasses.dataclass(kw_only=True)
class FileSystem:
    """
    Represents a file system for managing and querying files across
    various storage backends.

    :ivar root_path: The root directory path of the file system to search for files.
    For local storage this should be an absolute path (e.g., "/tmp").
    For AWS, S3 and GCS, this should be a bucket name and any subdirectories.
    (e.g. "my-bucket/subdirectory")
    For OCI, this should be a bucket@namespace and any subdirectories.
    (e.g. "my-bucket@my-namespace/subdirectory")
    For Azure, this should be a container name and any subdirectories.
    (e.g. "my-container/subdirectory")
    :type root_path: str
    :ivar storage_backend: The backend used for file storage. Defaults to
        `DEFAULT_STORAGE_BACKEND` if not specified.
    :type storage_backend: Optional[STORAGE_BACKEND_TYPE]
    :ivar accepted_file_extensions: The set of allowed file extensions for file
        operations, defaulting to a pre-defined set (`ALLOWED_FILE_EXTENSIONS`).
    :type accepted_file_extensions: Set[str]
    """

    root_path: str
    storage_backend: Optional[STORAGE_BACKEND_TYPE] = DEFAULT_STORAGE_BACKEND
    accepted_file_extensions: Set[str] = dataclasses.field(
        default_factory=lambda: set(ALLOWED_FILE_EXTENSIONS)
    )
    signed_url_expiration: int = 86400

    def __post_init__(self):
        if not self.root_path:
            raise ValueError("root_path cannot be empty")
        if not self.accepted_file_extensions:
            raise ValueError("accepted_file_extensions cannot be empty")

        for accepted_ext in self.accepted_file_extensions:
            if accepted_ext not in ALLOWED_FILE_EXTENSIONS:
                raise ValueError(
                    f"accepted_file_extension '{accepted_ext}' is not supported"
                )

        self._storage = create_storage(self.storage_backend)

    def _build_path(self, *paths: str) -> str:
        """
        Builds a filesystem path by joining the provided path components with the
        root path, considering the appropriate path-handling class depending
        on the storage backend and the operating system.

        :param paths: Components of the path to be joined with the root path.
        :type paths: str
        :return: A string representation of the constructed filesystem path.
        :rtype: str
        """
        pathlib_cls = pathlib.PurePath
        if self.storage_backend != STORAGE_BACKEND_LOCAL and sys.platform == "win32":
            pathlib_cls = pathlib.PurePosixPath

        return str(pathlib_cls(self.root_path, *paths))

    def _find_files_by_pattern(self, glob_pattern: str) -> List[str]:
        """
        Finds files in the storage system that match the provided glob pattern.

        :param glob_pattern: The glob pattern to match file paths against.
        :type glob_pattern: str
        :return: A list of file paths that match the provided glob pattern.
        :rtype: List[str]
        :raises exception.StorageException: If an error occurs while
                                            fetching the file paths.
        """
        try:
            search_path = self._build_path(glob_pattern)
            return self._storage.glob(search_path)
        except Exception as e:
            logger.exception(
                "File search failed",
                extra={
                    "glob_pattern": glob_pattern,
                    "error": str(e),
                },
            )
            raise exception.StorageException(
                f"Failed to find files matching pattern '{glob_pattern}': {str(e)}"
            ) from e

    def find_files(self) -> List[str]:
        """
        Finds and retrieves a list of file paths based on the accepted file extensions.

        :return: A list of file paths matching the accepted file extensions.
        :rtype: List[str]
        """
        patterns = [f"**/*.{ext}" for ext in self.accepted_file_extensions]
        all_files = []

        for pattern in patterns:
            files = self._find_files_by_pattern(pattern)
            all_files.extend(files)

        return all_files

    def retrieve_files_with_names(self) -> Optional[Dict[str, str]]:
        """
        Returns a dictionary containing file paths and their corresponding names.
        For bucket storage, the file paths are returned as URLs.
        """
        logger.info("Retrieving files from filesystem under %s", self.root_path)
        if files := self.find_files():
            return (
                {file: pathlib.Path(file).name for file in files}
                if self.storage_backend == STORAGE_BACKEND_LOCAL
                else {
                    self._storage.sign(
                        file, expiration=self.signed_url_expiration
                    ): pathlib.Path(file).name
                    for file in files
                }
            )
        return None
