#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

docker build -t dig-bioindex-dev .
docker tag dig-bioindex-dev gcr.io/broad-tools-development/dig-bioindex-dev
docker push gcr.io/broad-tools-development/dig-bioindex-dev
gcloud run deploy dig-bioindex-dev \
    --image gcr.io/broad-tools-development/dig-bioindex-dev \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --port 5000 \
    --add-cloudsql-instances=broad-tools-development:us-central1:dig-bioindex-dev
