#!/bin/bash
PROJECT_ROOT=$LANDALYTICS_PROJECT_ROOT
DATASOURCE=codepoint-open
SOURCE_CONFIG_LOCATION=$PROJECT_ROOT/scripts/$DATASOURCE/config/codepoint-open.conf
RUN_CONFIG_DIRECTORY=$PROJECT_ROOT/run/etl/$DATASOURCE/
RUN_CONFIG_LOCATION=$RUN_CONFIG_DIRECTORY/codepoint-open.conf

if [[ -z $PROJECT_ROOT ]]
then
  echo "PROJECT_ROOT is empty, skipping..."
else
  # Copy config to "run" location
  mkdir -p $RUN_CONFIG_DIRECTORY

  # Replace @PROJECTROOT@ with variable
  sed "s#@PROJECTROOT@#$PROJECT_ROOT#g" $SOURCE_CONFIG_LOCATION > $RUN_CONFIG_LOCATION

  spark-submit \
    --class com.landalytics.etl.codepointopen.CreateCaseClass \
    --master local[*] \
    $PROJECT_ROOT/data-source-codepoint-open/codepoint-open-etl/build/libs/codepoint-open-etl-shadow_2.12.jar \
    -c $RUN_CONFIG_LOCATION

fi