#!/bin/bash
PROJECT_ROOT=$LANDALYTICS_PROJECT_ROOT

if [[ -z $PROJECT_ROOT ]]
then
  echo "PROJECT_ROOT is empty, skipping..."
else
  spark-submit \
    --class com.landalytics.etl.landregistry.ImportRawToParquet \
    --master local[*] \
    $PROJECT_ROOT/data-source-land-registry/land-registry-etl/build/libs/land-registry-etl-shadow_2.12.jar \
    -s $PROJECT_ROOT/data/land-registry/csv/pp-complete.csv \
    -d $PROJECT_ROOT/data/land-registry/raw/land-regsitry.parquet
fi