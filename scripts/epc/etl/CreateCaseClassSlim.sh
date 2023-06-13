#!/bin/bash
PROJECT_ROOT=

if [ -z $PROJECT_ROOT ]
then
  echo "PROJECT_ROOT is empty, skipping..."
else
  spark-submit \
    --class com.landalytics.etl.epc.CreateCaseClassSlim \
    --master local[*] \
    --conf "spark.executor.extraJavaOptions=-Djava.library.path=$PROJECT_ROOT/libpostal/jniLibs" \
    --conf "spark.driver.extraJavaOptions=-Djava.library.path=$PROJECT_ROOT/libpostal/jniLibs" \
    $PROJECT_ROOT/data-source-epc/epc-etl/build/libs/epc-etl-shadow_2.12.jar \
    -s $PROJECT_ROOT/data/epc/raw/epc.parquet \
    -d $PROJECT_ROOT/data/epc/clean/epc-slim.parquet
fi
