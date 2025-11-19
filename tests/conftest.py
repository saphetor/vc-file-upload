import pytest


@pytest.fixture
def env_vars(monkeypatch):
    monkeypatch.setenv("AWS_S3_ACCESS_KEY_ID", "akid")
    monkeypatch.setenv("AWS_S3_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_S3_REGION_NAME", "eu-west-1")
    monkeypatch.setenv("GCP_PROJECT_ID", "my-project")
    monkeypatch.setenv("GCP_CREDENTIALS_FILE", "/home/user/creds.json")
    monkeypatch.setenv("OCI_CONFIG_FILE", "/home/user/.oci/config")
    monkeypatch.setenv("AZURE_ACCOUNT_NAME", "acct")
    monkeypatch.setenv("AZURE_ACCOUNT_KEY", "key123")
