# Transfer files to VarSome Clinical

**vc-file-upload** is both a Python library and a command-line tool that helps you transfer genomics-related files (VCF,
FASTQ and BAM) from a local filesystem or from popular cloud storage providers into the VarSome Clinical platform.

Supported storage backends: LOCAL, AWS S3, Google Cloud Storage (GCP), Oracle Cloud Infrastructure (OCI), and Azure
Blob Storage.

## What you can do

- CLI: Discover files under a folder or bucket path and submit them to VarSome Clinical either by uploading local files
  directly or by asking VarSome Clinical to fetch remote files via pre-signed URLs.
- Library (SDK): Embed the same functionality into your Python applications with a few lines of code.

## Quick links

- [Backends and required environment variables](docs/backends.md)
- [Using the library in your own code](docs/library.md)

## Installation options

### 1) Install with pip from this repository

Create a Python virtual environment and activate it, then run one of the following:

- From a cloned repository:

```bash
pip install .
```

- Directly from GitHub:

```bash
pip install "git+https://github.com/saphetor/vc-file-upload.git"
```

### 2) Use Docker (recommended for end users)

- Build locally:

```bash
git clone https://github.com/saphetor/vc-file-upload.git
cd vc-file-upload
docker build -t vc-file-upload:{{version}} .
```

- Or pull the prebuilt images from the GitHub Container Registry (GHCR):

```bash
docker pull ghcr.io/saphetor/vc-file-upload:latest
```

Note on running from the registry image:

- When you fetch the image from the registry, run it using the fully qualified name:

```bash
docker run ghcr.io/saphetor/vc-file-upload
```

- By default, Docker will use the latest tag if no tag is provided. For clarity, you can include the tag explicitly:

```bash
docker run ghcr.io/saphetor/vc-file-upload:latest
```

## CLI usage at a glance

- After installing with pip:

```bash
vc_file_upload --help
```

- With Docker:

```bash
docker run --rm ghcr.io/saphetor/vc-file-upload:latest --help
```

## Examples with Docker

### Local filesystem
Mount your local folder into the container and point to it with the LOCAL backend.

```bash
docker run --rm --env-file .env -v /local_folder:/data ghcr.io/saphetor/vc-file-upload:latest --backend LOCAL /data
```

### AWS S3

```bash
docker run --rm --env-file .env ghcr.io/saphetor/vc-file-upload:latest --backend AWS bucket/folder
```

### Google Cloud Storage
Mind permissions for the credentials file inside the container.

```bash
docker run --rm --env-file .env -v /service_account.json:/credentials.json \
  ghcr.io/saphetor/vc-file-upload:latest --backend GCP bucket/folder
```

Note: ensure the container user can read /credentials.json.

### Azure Blob Storage

```bash
docker run --rm --env-file .env ghcr.io/saphetor/vc-file-upload:latest --backend AZURE container/folder
```

### Oracle Cloud Infrastructure (OCI) Object Storage
Mount your ~/.oci or chosen OCI config directory.

```bash
docker run --rm --env-file .env -v /path-to/.oci/:/path-to/.oci/ \
  ghcr.io/saphetor/vc-file-upload:latest --backend OCI bucket@namespace/folder
```

## Environment variables

Place all sensitive variables in a .env file and pass it with --env-file when running Docker. See docs/backends.md for
the precise variables required by each backend. Examples:

- AWS: AWS_S3_ACCESS_KEY_ID, AWS_S3_SECRET_ACCESS_KEY, AWS_S3_REGION_NAME, AWS_S3_ENDPOINT_URL (optional)
- GCP: GCP_PROJECT_ID, GCP_CREDENTIALS_FILE (path inside the container)
- OCI: OCI_CONFIG_FILE (path inside the container)
- Azure: AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY
- VarSome Clinical: VCLIN_API_TOKEN

## Important notes on permissions for GCP/OCI

This container does not run as root. When binding credential files (GCP service account JSON, OCI config), ensure:

- The file is mounted inside the container under a path you reference via env vars.
- The container user can read it (e.g., world-readable file mode, or adjust UID/GID or bind mount options
  appropriately).

## Limitations and current behavior (temporary)

- Library response shape: When using the library and enabling multipart upload, the response structure does not
  currently match the other endpoints. This will be unified in a future release.

## For developers: quick start

- Install Git hooks (pre-commit):

```bash
pre-commit install
# Optional: run on entire repo once
pre-commit run --all-files
```

- Use Commitizen for commits (conventional commit messages):

```bash
cz commit
```

- Run tests:

```bash
pytest
```

- Lint/format:

```bash
black . && isort . && flake8
```

- Entry point for the CLI is declared in pyproject.toml
  (vc_file_upload = vc_file_upload.cli.transfer_files:main). The Docker image runs:

```bash
python -m vc_file_upload.cli.transfer_files
```

## License

See [LICENSE](LICENSE) file.
