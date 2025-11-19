import datetime
import pathlib
from typing import Optional, Union

from adlfs import AzureBlobFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from oci.object_storage.models import CreatePreauthenticatedRequestDetails
from ocifs import OCIFileSystem as BaseOCIFileSystem
from s3fs import S3FileSystem

from vc_file_upload import config
from vc_file_upload.exception import StorageException, UnknownStorageException
from vc_file_upload.logging_config import get_library_logger

logger = get_library_logger()


class OCIFileSystem(BaseOCIFileSystem):

    def sign(self, path, expiration=100, **kwargs):
        bucket, namespace, object_name = self.split_path(path)
        expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            seconds=expiration
        )
        access_type = CreatePreauthenticatedRequestDetails.ACCESS_TYPE_OBJECT_READ
        pre_request = CreatePreauthenticatedRequestDetails(
            name=pathlib.Path(object_name).name,
            access_type=access_type,
            time_expires=expires,
            object_name=object_name,
        )
        response = self.oci_client.create_preauthenticated_request(
            namespace_name=namespace,
            bucket_name=bucket,
            create_preauthenticated_request_details=pre_request,
        )
        return response.data.full_path


def create_storage(
    backend: Optional[config.STORAGE_BACKEND_TYPE] = None, *args, **kwargs
) -> Union[
    S3FileSystem, GCSFileSystem, OCIFileSystem, AzureBlobFileSystem, LocalFileSystem
]:
    """
    Creates a storage filesystem instance based on the specified or default backend.

    :param backend: The storage backend to use
    :param args: Additional positional arguments to be passed
                 to the storage backend constructor.
    :param kwargs: Additional keyword arguments to be passed to the storage backend
                   constructor or overridden by backend-specific defaults.
    :return: A filesystem instance corresponding to the specified storage backend,
             such as S3FileSystem, GCSFileSystem, OCIFileSystem, AzureBlobFileSystem,
            or LocalFileSystem.
    :rtype: Union[S3FileSystem, GCSFileSystem, OCIFileSystem,
            AzureBlobFileSystem, LocalFileSystem]
    :raises UnknownStorageException: If the specified storage backend
                                     is unknown or unsupported.
    """
    if backend is None:
        backend = config.DEFAULT_STORAGE_BACKEND

    backend_kwargs = config.get_storage_config(backend)
    for key, value in backend_kwargs.items():
        kwargs.setdefault(key, value)
    try:
        if backend == config.STORAGE_BACKEND_AWS:
            return S3FileSystem(*args, **kwargs)
        elif backend == config.STORAGE_BACKEND_GCP:
            return GCSFileSystem(*args, **kwargs)
        elif backend == config.STORAGE_BACKEND_OCI:
            return OCIFileSystem(*args, **kwargs)
        elif backend == config.STORAGE_BACKEND_AZURE:
            return AzureBlobFileSystem(*args, **kwargs)
        elif backend == config.STORAGE_BACKEND_LOCAL:
            return LocalFileSystem(*args, **kwargs)
    except Exception as e:
        logger.exception(
            "Failed to create storage filesystem",
            extra={
                "backend": backend,
                "error": str(e),
            },
        )
        raise StorageException(
            f"Failed to create storage filesystem for backend '{backend}': {str(e)}"
        ) from e
    logger.error("Unknown storage backend requested", extra={"backend": backend})
    raise UnknownStorageException(f"Unknown storage backend: {backend}")
