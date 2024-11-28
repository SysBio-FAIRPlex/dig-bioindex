FROM --platform=linux/amd64 python:3.8-slim as build

RUN apt-get update && \
    apt-get install -y default-libmysqlclient-dev pkg-config build-essential

COPY requirements.txt .
COPY bioindex ./bioindex
COPY .bioindex ./.bioindex
COPY web ./web
COPY schema.graphql ./schema.graphql

ENV GOOGLE_CLOUD_PROJECT=broad-tools-development

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 5000

# Command to run both cloud_sql_proxy and your app
CMD ["python3", "-m", "bioindex.main", "serve"]
