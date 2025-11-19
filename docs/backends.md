# Backends and required environment variables

This project supports five storage backends. Configure them using environment variables. For Docker, place sensitive
values in a .env file and pass it via --env-file .env.

## Common

- VCLIN_API_TOKEN: VarSome Clinical API token used by the CLI and library uploader.

## LOCAL

- No storage-specific variables are required.
- Root path example: /data or C:\\data

## AWS S3 (backend: AWS)

- AWS_S3_ACCESS_KEY_ID: Your AWS access key ID.
- AWS_S3_SECRET_ACCESS_KEY: Your AWS secret access key.
- AWS_S3_REGION_NAME: The region of the bucket (e.g., us-east-1).
- AWS_S3_ENDPOINT_URL: Optional. Custom endpoint (e.g., for S3-compatible storage); defaults to https://s3.<region>.amazonaws.com.
- Root path example: my-bucket/path/subdir

## Google Cloud Storage (backend: GCP)

- GCP_PROJECT_ID: The GCP project ID.
- GCP_CREDENTIALS_FILE: Absolute path to a service account JSON inside the runtime/container.
- Root path example: my-gcs-bucket/path/subdir

Notes:

- When running in Docker, mount the credentials file and ensure it is readable by the container user (the image does not run as root).
- Example mount:

```bash
-v /host/path/service_account.json:/credentials.json
```

And set in .env:

```bash
GCP_CREDENTIALS_FILE=/credentials.json
```

## Oracle Cloud Infrastructure (backend: OCI)

- OCI_CONFIG_FILE: Absolute path to an OCI config file inside the runtime/container (e.g., /home/appuser/.oci/config). The file should point to key_file, tenancy, user, fingerprint, and region as per OCI SDK.
- Root path format: bucket@namespace/path/subdir

Notes:

- When running in Docker, mount your .oci directory and ensure files are readable by the container user. You may also need to adjust paths inside the config file to match the container's filesystem layout or mount point.
- Example:

```bash
-v /home/you/.oci/:/home/appuser/.oci/
```

Set in .env:

```bash
OCI_CONFIG_FILE=/home/appuser/.oci/config
```

## Azure Blob Storage (backend: AZURE)

- AZURE_ACCOUNT_NAME: Name of your storage account.
- AZURE_ACCOUNT_KEY: Key for the storage account.
- Root path example: my-container/path/subdir

## Example .env snippets

### VarSome Clinical

```bash
VCLIN_API_TOKEN=YOUR_TOKEN_HERE
```

### AWS

```bash
AWS_S3_ACCESS_KEY_ID=AKIA...
AWS_S3_SECRET_ACCESS_KEY=...
AWS_S3_REGION_NAME=us-east-1
# AWS_S3_ENDPOINT_URL=https://minio.example.com
```

### GCP

```bash
GCP_PROJECT_ID=my-gcp-project
GCP_CREDENTIALS_FILE=/credentials.json
```

### OCI

```bash
OCI_CONFIG_FILE=/home/appuser/.oci/config
```

### Azure

```bash
AZURE_ACCOUNT_NAME=myaccount
AZURE_ACCOUNT_KEY=...
```

## Root path reminder by backend

- LOCAL: absolute filesystem path inside the host/container (mount it when using Docker).
- AWS: bucket[/subpath]
- GCP: bucket[/subpath]
- OCI: bucket@namespace[/subpath]
- AZURE: container[/subpath]

## Security and permissions reminders

- Never bake secrets into the image. Use --env-file for Docker, or environment variables in your execution environment.
- GCP/OCI files must be readable by the non-root container user. Adjust file permissions (e.g., chmod 0644) or use appropriate bind-mount options.
