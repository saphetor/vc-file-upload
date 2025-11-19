FROM python:3.14-slim

RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

COPY requirements.txt ./
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY vc_file_upload ./vc_file_upload

RUN chown -R appuser:appuser /app

USER appuser

ENTRYPOINT ["python", "-m", "vc_file_upload.cli.transfer_files"]

CMD ["--help"]
