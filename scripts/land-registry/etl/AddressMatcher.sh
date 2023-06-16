#!/bin/bash
PROJECT_ROOT=$LANDALYTICS_PROJECT_ROOT
DATASOURCE=land-registry
SOURCE_CONFIG_LOCATION=$PROJECT_ROOT/scripts/$DATASOURCE/config/address-matcher.conf
RUN_CONFIG_DIRECTORY=$PROJECT_ROOT/run/address-matcher/$DATASOURCE/
RUN_CONFIG_LOCATION=$RUN_CONFIG_DIRECTORY/address-matcher.conf

if [[ -z $PROJECT_ROOT ]]
then
  echo "PROJECT_ROOT is empty, skipping..."
else
  # Copy config to "run" location
  mkdir -p $RUN_CONFIG_DIRECTORY

  # Replace @PROJECTROOT@ with variable
  sed "s#@PROJECTROOT@#$PROJECT_ROOT#g" $SOURCE_CONFIG_LOCATION > $RUN_CONFIG_LOCATION

  spark-submit \
    --class com.landalytics.addressmatcher.AddressMatcher \
    --master local[*] \
    $PROJECT_ROOT/landalytics-address-matcher/address-matcher/build/libs/address-matcher-shadow_2.12.jar \
    -c $RUN_CONFIG_LOCATION

fi