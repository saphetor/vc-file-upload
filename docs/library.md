# Using vc-file-upload as a library

The library exposes two main building blocks:

- FileSystem: abstracts discovery and signing of files across different storage backends.
- VarSomeClinicalFileUploader: uploads local files or requests VarSome Clinical to retrieve remote files through signed URLs.

## Installation

```bash
pip install .  # from a cloned repo
# or install from your Git repository URL if available
pip install "git+https://github.com/saphetor/vc-file-upload.git"
```

## Basic usage

### 1) Upload local files discovered under a directory

```python
import os
from vc_file_upload.filesystem import FileSystem
from vc_file_upload.varsome import VarSomeClinicalFileUploader

# Required: export VCLIN_API_TOKEN in your environment

fs = FileSystem(
    root_path="/data",                     # absolute local directory
    storage_backend="LOCAL",
)

uploader = VarSomeClinicalFileUploader(
    clinical_api_token=os.environ["VCLIN_API_TOKEN"],
    clinical_base_url="https://ch.clinical.varsome.com",
)

files = fs.retrieve_files_with_names()  # {"/data/file.vcf.gz": "file.vcf.gz", ...}
if files:
    result = uploader.upload_local_files(files)
    print(result)
```

### 2) Ask VarSome Clinical to retrieve files from a cloud backend

```python
import os
from vc_file_upload.filesystem import FileSystem
from vc_file_upload.varsome import VarSomeClinicalFileUploader

# Ensure backend-specific environment variables are set (see docs/backends.md)

fs = FileSystem(
    root_path="my-bucket/path",           # see backend-specific root formats
    storage_backend="AWS",                # or GCP, OCI, AZURE
    signed_url_expiration=86400,
)

uploader = VarSomeClinicalFileUploader(
    clinical_api_token=os.environ["VCLIN_API_TOKEN"],
)

files = fs.retrieve_files_with_names()  # {"https://signed.url": "file.vcf.gz", ...}
if files:
    result = uploader.retrieve_external_files(files)
    print(result)
```

## Selecting accepted file extensions

FileSystem accepts a set of extensions via accepted_file_extensions. Defaults: {"vcf", "vcf.gz", "fastq.gz", "bam"}.

```python
from vc_file_upload.filesystem import FileSystem

fs = FileSystem(
    root_path="/data",
    storage_backend="LOCAL",
    accepted_file_extensions={"vcf", "vcf.gz"},
)
```

## Notes on multipart uploads

- In the CLI shipped with this project, multipart uploads are currently disabled until API endpoint improvements are available.

## Backend configuration helpers

The library reads configuration for each backend from environment variables (see [vc_file_upload/config.py](../vc_file_upload/config.py) and [docs/backends.md](../docs/backends.md)). You generally do not need to pass credentials programmatically; set them in the environment.

## Troubleshooting

- Ensure VCLIN_API_TOKEN is set.
- For remote backends, verify your credentials and permissions. In containers, ensure credential files are readable by the non-root user.
- Check allowed extensions, or set accepted_file_extensions accordingly.
