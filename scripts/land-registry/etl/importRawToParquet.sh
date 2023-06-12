#!/bin/bash
PROJECT_ROOT=PATH_TO_PROJECT_ROOT

spark-submit \
  --class com.landalytics.etl.landregistry.ImportRawToParquet \
  --master local[*] \
  $PROJECT_ROOT/data-source-land-registry/land-registry-etl/build/libs/land-registry-etl-shadow_2.12.jar> \
  -s $PROJECT_ROOT/data/land-registry/csv/pp-complete.csv \
  -d $PROJECT_ROOT/data/land-registry/raw/land-regsitry.parquet
