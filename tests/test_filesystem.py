import pathlib
from unittest.mock import MagicMock, patch

import pytest

from vc_file_upload import exception
from vc_file_upload.config import (
    STORAGE_BACKEND_AWS,
    STORAGE_BACKEND_LOCAL,
)
from vc_file_upload.filesystem import FileSystem


@pytest.fixture
def mock_storage():
    storage = MagicMock()
    storage.glob = MagicMock()
    storage.sign = MagicMock(
        side_effect=lambda p, expiration=86400: f"signed:{p}:{expiration}"
    )
    return storage


@pytest.fixture
def patch_create_storage(mock_storage):
    with patch("vc_file_upload.filesystem.create_storage", return_value=mock_storage):
        yield mock_storage


def test_init_validations():
    with pytest.raises(ValueError):
        FileSystem(root_path="")

    with pytest.raises(ValueError):
        FileSystem(root_path="/data", accepted_file_extensions=set())

    with pytest.raises(ValueError):
        FileSystem(root_path="/data", accepted_file_extensions={"weirdext"})


@pytest.mark.parametrize(
    "backend, expected_path_cls",
    [
        (STORAGE_BACKEND_LOCAL, pathlib.PurePath),  # local uses OS default
        (STORAGE_BACKEND_AWS, pathlib.PurePosixPath),  # non-local + win32 uses posix
    ],
)
def test_build_path_handles_backends_and_platform(
    backend, expected_path_cls, patch_create_storage
):
    fs = FileSystem(
        root_path="C:/root" if backend == STORAGE_BACKEND_AWS else "/root",
        storage_backend=backend,
    )

    with patch("vc_file_upload.filesystem.sys.platform", "win32"):
        built = fs._build_path("sub", "file.vcf")

    expected = str(expected_path_cls(fs.root_path, "sub", "file.vcf"))
    assert built == expected


def test_find_files_success_aggregates_patterns(patch_create_storage, mock_storage):
    fs = FileSystem(
        root_path="/data",
        accepted_file_extensions={"vcf", "bam"},
        storage_backend=STORAGE_BACKEND_LOCAL,
    )

    # Configure glob responses for two patterns (order-independent)
    def glob_side_effect(arg):
        if arg.endswith("**/*.vcf"):
            return ["/data/a/a.vcf", "/data/b/b.vcf"]
        if arg.endswith("**/*.bam"):
            return ["/data/c/c.bam"]
        return []

    mock_storage.glob.side_effect = glob_side_effect

    files = fs.find_files()

    assert sorted(files) == sorted(["/data/a/a.vcf", "/data/b/b.vcf", "/data/c/c.bam"])

    called_args = [args[0] for args, _ in mock_storage.glob.call_args_list]
    assert any(p.endswith("**/*.vcf") for p in called_args)
    assert any(p.endswith("**/*.bam") for p in called_args)


def test_find_files_wraps_storage_errors(patch_create_storage, mock_storage):
    fs = FileSystem(root_path="/data")
    mock_storage.glob.side_effect = Exception("boom")

    with pytest.raises(exception.StorageException) as ei:
        fs._find_files_by_pattern("**/*.vcf")

    msg = str(ei.value)
    assert "Failed to find files matching pattern" in msg
    assert "boom" in msg


def test_retrieve_files_with_names_none_when_no_files(
    patch_create_storage, mock_storage
):
    fs = FileSystem(root_path="/data")
    mock_storage.glob.return_value = []

    result = fs.retrieve_files_with_names()
    assert result is None

    assert mock_storage.glob.called


@pytest.mark.usefixtures("patch_create_storage")
def test_retrieve_files_with_names_local_no_sign(mock_storage):
    fs = FileSystem(root_path="/root", storage_backend=STORAGE_BACKEND_LOCAL)
    mock_storage.glob.side_effect = lambda pattern: (
        [
            "/root/dir1/sample-1.vcf",
            "/root/dir2/run.bam",
        ]
        if pattern.endswith("**/*.vcf")
        else (["/root/dir2/run.bam"] if pattern.endswith("**/*.bam") else [])
    )

    result = fs.retrieve_files_with_names()

    assert result == {
        "/root/dir1/sample-1.vcf": "sample-1.vcf",
        "/root/dir2/run.bam": "run.bam",
    }
    mock_storage.sign.assert_not_called()


@pytest.mark.usefixtures("patch_create_storage")
def test_retrieve_files_with_names_bucket_uses_signed_urls(mock_storage):
    fs = FileSystem(
        root_path="bucket/path",
        storage_backend=STORAGE_BACKEND_AWS,
        signed_url_expiration=3600,
        accepted_file_extensions={"vcf", "bam"},
    )

    def glob_side_effect(pattern):
        if pattern.endswith("**/*.vcf"):
            return ["bucket/path/a.vcf"]
        if pattern.endswith("**/*.bam"):
            return ["bucket/path/b.bam"]
        return []

    mock_storage.glob.side_effect = glob_side_effect

    result = fs.retrieve_files_with_names()

    mock_storage.sign.assert_any_call("bucket/path/a.vcf", expiration=3600)
    mock_storage.sign.assert_any_call("bucket/path/b.bam", expiration=3600)

    assert result == {
        "signed:bucket/path/a.vcf:3600": "a.vcf",
        "signed:bucket/path/b.bam:3600": "b.bam",
    }
