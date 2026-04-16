#!/bin/bash

# This script is used to download the flights dataset from S3 and unzip it into the data directory.
# It assumes that the AWS CLI is installed and configured properly.

# Usage: 
#   bash ./ingestion.sh

source ./config.sh

echo "Descargando de: s3://$S3_BUCKET/flights.zip"
echo "Directorio destino: $DATA_DIR"

aws s3 cp s3://$S3_BUCKET/flights.zip . --no-sign-request
unzip flights.zip -d $DATA_DIR/

echo "Dataset descargado y descomprimido en $DATA_DIR/"