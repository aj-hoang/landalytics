#!/bin/bash
PROJECT_ROOT=

if [ -z $PROJECT_ROOT ]
then
  echo "PROJECT_ROOT is empty, skipping..."
else
  spark-submit \
    --class com.landalytics.etl.epc.ImportRawToParquet \
    --master local[*] \
    $PROJECT_ROOT/data-source-epc/epc-etl/build/libs/epc-etl-shadow_2.12.jar \
    -s $PROJECT_ROOT/data/epc/csv/certificates.csv \
    -d $PROJECT_ROOT/data/epc/raw/epc.parquet
fi
