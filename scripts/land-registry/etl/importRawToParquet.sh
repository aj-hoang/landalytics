#!/bin/bash

spark-submit \
  --class com.landalytics.etl.landregistry.ImportRawToParquet \
  --master local[*] \
  /home/anthony/temp/land-registry-etl-shadow_2.12.jar \
  -s <source-uri> \
  -d <destination-uri>
