FROM --platform=linux/amd64 python:3.12-slim as build

RUN apt-get update && \
    apt-get install -y default-libmysqlclient-dev pkg-config build-essential

COPY requirements.txt .
COPY bioindex ./bioindex
COPY batch-index-files/index_files.py .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install mysqlclient

ENTRYPOINT gunicorn bioindex.server:app --workers 8 -k uvicorn.workers.UvicornWorker --timeout 0
