import logging

import pytest

from vc_file_upload import config as cfg


def test_constants_and_allowed_extensions():
    assert cfg.STORAGE_BACKEND_AWS == "AWS"
    assert cfg.STORAGE_BACKEND_GCP == "GCP"
    assert cfg.STORAGE_BACKEND_OCI == "OCI"
    assert cfg.STORAGE_BACKEND_AZURE == "AZURE"
    assert cfg.STORAGE_BACKEND_LOCAL == "LOCAL"

    assert cfg.DEFAULT_STORAGE_BACKEND == cfg.STORAGE_BACKEND_LOCAL

    assert {"vcf", "vcf.gz", "fastq.gz", "bam"}.issubset(cfg.ALLOWED_FILE_EXTENSIONS)


def test_get_aws_config_reads_env(monkeypatch):
    monkeypatch.setenv("AWS_S3_ACCESS_KEY_ID", "akid")
    monkeypatch.setenv("AWS_S3_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_S3_REGION_NAME", "eu-west-1")

    cfg_dict = cfg.get_aws_config()
    assert cfg_dict == {
        "key": "akid",
        "secret": "secret",
        "client_kwargs": {
            "region_name": "eu-west-1",
            "endpoint_url": "https://s3.eu-west-1.amazonaws.com",
        },
    }


def test_get_gcp_config_reads_env(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "my-project")
    monkeypatch.setenv("GCP_CREDENTIALS_FILE", "/tmp/creds.json")

    cfg_dict = cfg.get_gcp_config()
    assert cfg_dict == {
        "project": "my-project",
        "token": "/tmp/creds.json",
    }


def test_get_oci_config_reads_env(monkeypatch):
    monkeypatch.setenv("OCI_CONFIG_FILE", "/home/user/.oci/config")

    cfg_dict = cfg.get_oci_config()
    assert cfg_dict == {"config": "/home/user/.oci/config"}


def test_get_azure_config_reads_env(monkeypatch):
    monkeypatch.setenv("AZURE_ACCOUNT_NAME", "acct")
    monkeypatch.setenv("AZURE_ACCOUNT_KEY", "key123")

    cfg_dict = cfg.get_azure_config()
    assert cfg_dict == {
        "account_name": "acct",
        "account_key": "key123",
    }


@pytest.mark.parametrize(
    "backend, expected",
    [
        (
            cfg.STORAGE_BACKEND_AWS,
            {
                "key": "akid",
                "secret": "secret",
                "client_kwargs": {
                    "region_name": "eu-west-1",
                    "endpoint_url": "https://s3.eu-west-1.amazonaws.com",
                },
            },
        ),
        (
            cfg.STORAGE_BACKEND_GCP,
            {"project": "my-project", "token": "/home/user/creds.json"},
        ),
        (cfg.STORAGE_BACKEND_OCI, {"config": "/home/user/.oci/config"}),
        (cfg.STORAGE_BACKEND_AZURE, {"account_name": "acct", "account_key": "key123"}),
        (cfg.STORAGE_BACKEND_LOCAL, {}),
    ],
)
@pytest.mark.usefixtures("env_vars")
def test_get_storage_config_dispatch(backend, expected):
    assert cfg.get_storage_config(backend) == expected


def test_get_storage_config_unknown_backend_logs_warning(caplog):
    unknown = "FOO"
    with caplog.at_level(logging.WARNING, logger="vc_file_upload"):
        result = cfg.get_storage_config(unknown)

    assert result == {}

    records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any(
        r.getMessage() == "Unknown storage backend requested"
        and getattr(r, "backend", None) == unknown
        for r in records
    )
