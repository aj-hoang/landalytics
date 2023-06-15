#!/bin/bash
PROJECT_ROOT=$LANDALYTICS_PROJECT_ROOT
DATASOURCE=os-open-uprn
SOURCE_CONFIG_LOCATION=$PROJECT_ROOT/scripts/$DATASOURCE/config/os-open-uprn.conf
RUN_CONFIG_DIRECTORY=$PROJECT_ROOT/run/etl/$DATASOURCE/
RUN_CONFIG_LOCATION=$RUN_CONFIG_DIRECTORY/os-open-uprn.conf

if  [ -z $PROJECT_ROOT ]
then
  echo "PROJECT_ROOT is empty, skipping..."
else
  # Copy config to "run" location
  mkdir -p $RUN_CONFIG_DIRECTORY

  # Replace @PROJECTROOT@ with variable
  sed "s#@PROJECTROOT@#$PROJECT_ROOT#g" $SOURCE_CONFIG_LOCATION > $RUN_CONFIG_LOCATION

  spark-submit \
    --class com.landalytics.etl.osopenuprn.CreateCaseClass \
    --master local[*] \
    --executor-memory 2G \
    --driver-memory 2G \
    --num-executors 4 \
    $PROJECT_ROOT/data-source-os-open-uprn/os-open-uprn-etl/build/libs/os-open-uprn-etl-shadow_2.12.jar \
    -c $RUN_CONFIG_LOCATION

fi
