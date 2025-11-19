from unittest.mock import patch

import pytest

from vc_file_upload import config as cfg
from vc_file_upload.exception import StorageException, UnknownStorageException
from vc_file_upload.storage import (
    AzureBlobFileSystem,
    GCSFileSystem,
    LocalFileSystem,
    OCIFileSystem,
    S3FileSystem,
    create_storage,
)


@pytest.fixture
def patch_cloud_filesystems_init():
    """
    Avoid the filesystem initialization that triggers network calls.
    :return:
    """
    with patch.object(
        AzureBlobFileSystem, "__init__", return_value=None
    ) as mock_azure_init:
        with patch.object(
            OCIFileSystem, "__init__", return_value=None
        ) as mock_oci_init:
            with patch.object(
                S3FileSystem, "__init__", return_value=None
            ) as mock_s3_init:
                with patch.object(
                    GCSFileSystem, "__init__", return_value=None
                ) as mock_gcs_init:
                    with patch.object(
                        LocalFileSystem, "__init__", return_value=None
                    ) as mock_local_init:
                        yield {
                            cfg.STORAGE_BACKEND_AWS: mock_s3_init,
                            cfg.STORAGE_BACKEND_GCP: mock_gcs_init,
                            cfg.STORAGE_BACKEND_OCI: mock_oci_init,
                            cfg.STORAGE_BACKEND_AZURE: mock_azure_init,
                            cfg.STORAGE_BACKEND_LOCAL: mock_local_init,
                        }


@pytest.mark.parametrize(
    "backend, expected",
    {
        (cfg.STORAGE_BACKEND_AWS, S3FileSystem),
        (cfg.STORAGE_BACKEND_GCP, GCSFileSystem),
        (cfg.STORAGE_BACKEND_OCI, OCIFileSystem),
        (cfg.STORAGE_BACKEND_AZURE, AzureBlobFileSystem),
        (cfg.STORAGE_BACKEND_LOCAL, LocalFileSystem),
    },
)
@pytest.mark.usefixtures("env_vars", "patch_cloud_filesystems_init")
def test_create_storage_dispatch_and_kwargs_merge(backend, expected):
    assert isinstance(create_storage(backend), expected)


def test_create_storage_default_local_storage():
    assert isinstance(create_storage(), LocalFileSystem)


def test_create_unknown_storage_raises_exception():
    with pytest.raises(UnknownStorageException):
        create_storage("FOO")


def test_create_storage_failure_raises_exception(patch_cloud_filesystems_init):
    with patch("vc_file_upload.storage.LocalFileSystem") as mock_create_storage:
        mock_create_storage.side_effect = Exception("something went wrong")
        with pytest.raises(StorageException):
            create_storage(cfg.STORAGE_BACKEND_LOCAL)


@pytest.mark.parametrize(
    "storage, kwargs, expected_kwargs",
    [
        (cfg.STORAGE_BACKEND_LOCAL, {"key": "value"}, {"key": "value"}),
        (
            cfg.STORAGE_BACKEND_AZURE,
            {"account_name": "another_acct"},
            {"account_name": "another_acct", "account_key": "key123"},
        ),
        (
            cfg.STORAGE_BACKEND_GCP,
            {"project": "my-project2"},
            {"project": "my-project2", "token": "/home/user/creds.json"},
        ),
        (
            cfg.STORAGE_BACKEND_OCI,
            {"config": "/home/user/.oci/config2"},
            {"config": "/home/user/.oci/config2"},
        ),
        (
            cfg.STORAGE_BACKEND_AWS,
            {"key": "new_akid", "client_kwargs": {"region_name": "eu-east-1"}},
            {
                "key": "new_akid",
                "secret": "secret",
                "client_kwargs": {"region_name": "eu-east-1"},
            },
        ),
    ],
)
@pytest.mark.usefixtures("env_vars")
def test_create_storage_kwargs_merge(
    storage, patch_cloud_filesystems_init, kwargs, expected_kwargs
):
    create_storage(storage, **kwargs)
    patch_cloud_filesystems_init[storage].assert_called_with(**expected_kwargs)
