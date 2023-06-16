#!/bin/bash
PROJECT_ROOT=$LANDALYTICS_PROJECT_ROOT

if [[ -z $PROJECT_ROOT ]]
then
  echo "PROJECT_ROOT is empty, skipping..."
else
  spark-submit \
    --class com.landalytics.etl.landregistry.CreateCaseClass \
    --master local[*] \
    --conf "spark.executor.extraJavaOptions=-Djava.library.path=$PROJECT_ROOT/libpostal/jniLibs" \
    --conf "spark.driver.extraJavaOptions=-Djava.library.path=$PROJECT_ROOT/libpostal/jniLibs" \
    $PROJECT_ROOT/data-source-land-registry/land-registry-etl/build/libs/land-registry-etl-shadow_2.12.jar \
    -s $PROJECT_ROOT/data/land-registry/raw/land-registry.parquet \
    -d $PROJECT_ROOT/data/land-registry/clean/land-registry.parquet
fi