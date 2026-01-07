#!/usr/bin/env python3
import argparse
import os
import sys
from typing import Dict, Optional, Set

from vc_file_upload.cli.logger import logger
from vc_file_upload.config import (
    ALLOWED_FILE_EXTENSIONS,
    DEFAULT_STORAGE_BACKEND,
    STORAGE_BACKEND_AWS,
    STORAGE_BACKEND_AZURE,
    STORAGE_BACKEND_GCP,
    STORAGE_BACKEND_LOCAL,
    STORAGE_BACKEND_OCI,
)
from vc_file_upload.filesystem import FileSystem
from vc_file_upload.varsome import VarSomeClinicalFileUploader


def _parse_extensions(extensions: str) -> Set[str]:
    """
    Parses a comma-separated list of extensions into a normalized set.
    Leading dots are removed and values are lowercased. Only allowed
    extensions are retained.
    """
    extensions = {
        e.strip().lower().lstrip(".") for e in extensions.split(",") if e.strip()
    }
    return {e for e in extensions if e in ALLOWED_FILE_EXTENSIONS}


def _create_filesystem(
    root_path: str,
    backend: str,
    accepted_extensions: Set[str],
    signed_url_expiration: int,
) -> FileSystem:
    """
    Instantiates a FileSystem for the selected backend and root path.
    """
    return FileSystem(
        root_path=root_path,
        storage_backend=backend,
        accepted_file_extensions=accepted_extensions or set(ALLOWED_FILE_EXTENSIONS),
        signed_url_expiration=signed_url_expiration,
    )


def _create_uploader(base_url: str) -> VarSomeClinicalFileUploader:
    """
    Instantiates a VarSomeClinicalFileUploader using the token from the environment
    and the provided base URL.
    """
    if token := os.getenv("VCLIN_API_TOKEN"):
        return VarSomeClinicalFileUploader(
            clinical_api_token=token, clinical_base_url=base_url
        )
    else:
        raise ValueError(
            "Missing VarSome Clinical API token. Set one for VCLIN_API_TOKEN"
        )


def _transfer(
    backend: str,
    fs: FileSystem,
    uploader: VarSomeClinicalFileUploader,
) -> Dict[str, Optional[dict]]:
    """
    Executes the transfer based on the storage backend. For local storage it performs
    uploads of local files. For remote backends, it requests retrieval of external files
    via pre-signed URLs.
    """
    files = fs.retrieve_files_with_names()
    if not files:
        logger.info("No files found to transfer", extra={"root_path": fs.root_path})
        return {}

    logger.info(
        "Discovered files for transfer",
        extra={"count": len(files), "backend": backend, "root_path": fs.root_path},
    )

    if backend == STORAGE_BACKEND_LOCAL:
        logger.info("Uploading local files to VarSome Clinical")
        return uploader.upload_local_files(files)
    else:
        logger.info("Requesting VarSome Clinical to retrieve external files")
        return uploader.retrieve_external_files(files)


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Builds the argument parser for the transfer CLI, allowing the user to select
    a filesystem backend and a root path, along with optional tuning parameters.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Transfer files from a local directory or a cloud storage provider "
            "to VarSome Clinical"
        )
    )
    parser.add_argument(
        "root_path",
        help=(
            "Root path for the filesystem. For LOCAL provide an absolute directory. "
            "For cloud backends provide bucket/container and optional subpath."
        ),
    )
    parser.add_argument(
        "--backend",
        default=DEFAULT_STORAGE_BACKEND,
        choices=[
            STORAGE_BACKEND_LOCAL,
            STORAGE_BACKEND_AWS,
            STORAGE_BACKEND_GCP,
            STORAGE_BACKEND_OCI,
            STORAGE_BACKEND_AZURE,
        ],
        help="Filesystem backend to use (default: %(default)s)",
    )
    parser.add_argument(
        "--vclin-base-url",
        default="https://ch.clinical.varsome.com",
        help="VarSome Clinical base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--accepted-file-extensions",
        default="vcf,vcf.gz,fastq.gz,bam",
        help="Comma-separated list of accepted file extensions (default: %(default)s)",
    )
    parser.add_argument(
        "--signed-url-expiration",
        type=int,
        default=86400,
        help="Signed URL expiration in seconds for "
        "remote backends (default: %(default)s)",
    )
    return parser


def main(argv: Optional[list] = None) -> int:
    """
    Entry point for the transfer CLI. Parses arguments, creates the filesystem and
    uploader, and performs the transfer.
    """
    argv = sys.argv[1:] if argv is None else argv
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        accepted = _parse_extensions(args.accepted_file_extensions)
        if not accepted:
            accepted = set(ALLOWED_FILE_EXTENSIONS)
            logger.info(
                "Using default accepted extensions",
                extra={"extensions": sorted(accepted)},
            )

        fs = _create_filesystem(
            root_path=args.root_path,
            backend=args.backend,
            accepted_extensions=accepted,
            signed_url_expiration=args.signed_url_expiration,
        )

        uploader = _create_uploader(args.vclin_base_url)
        result = _transfer(args.backend, fs, uploader)

        logger.info(
            "Transfer completed",
            extra={"files_processed": len(result)},
        )
        return 0
    except Exception as e:
        logger.exception("Transfer failed", extra={"error": str(e)})
        return 1


if __name__ == "__main__":
    sys.exit(main())
