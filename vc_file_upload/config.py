import os
from typing import Literal

from vc_file_upload.logging_config import get_library_logger

logger = get_library_logger()

STORAGE_BACKEND_AWS = "AWS"
STORAGE_BACKEND_GCP = "GCP"
STORAGE_BACKEND_OCI = "OCI"
STORAGE_BACKEND_AZURE = "AZURE"
STORAGE_BACKEND_LOCAL = "LOCAL"
DEFAULT_STORAGE_BACKEND = STORAGE_BACKEND_LOCAL

STORAGE_BACKEND_TYPE = Literal["AWS", "GCP", "OCI", "AZURE", "LOCAL"]

ALLOWED_FILE_EXTENSIONS = {"vcf", "vcf.gz", "fastq.gz", "bam"}


def get_aws_config():
    region = os.getenv("AWS_S3_REGION_NAME")
    return {
        "key": os.getenv("AWS_S3_ACCESS_KEY_ID"),
        "secret": os.getenv("AWS_S3_SECRET_ACCESS_KEY"),
        "client_kwargs": {
            "region_name": region,
            "endpoint_url": os.getenv(
                "AWS_S3_ENDPOINT_URL", f"https://s3.{region}.amazonaws.com"
            ),
        },
    }


def get_gcp_config():
    return {
        "project": os.getenv("GCP_PROJECT_ID"),
        "token": os.getenv("GCP_CREDENTIALS_FILE"),
    }


def get_oci_config():
    return {
        "config": os.getenv("OCI_CONFIG_FILE"),
    }


def get_azure_config():
    return {
        "account_name": os.getenv("AZURE_ACCOUNT_NAME"),
        "account_key": os.getenv("AZURE_ACCOUNT_KEY"),
    }


def get_storage_config(backend):
    config_getters = {
        STORAGE_BACKEND_AWS: get_aws_config,
        STORAGE_BACKEND_GCP: get_gcp_config,
        STORAGE_BACKEND_OCI: get_oci_config,
        STORAGE_BACKEND_AZURE: get_azure_config,
        STORAGE_BACKEND_LOCAL: lambda: {},
    }

    if getter := config_getters.get(backend):
        return getter()
    logger.warning("Unknown storage backend requested", extra={"backend": backend})
    return {}
