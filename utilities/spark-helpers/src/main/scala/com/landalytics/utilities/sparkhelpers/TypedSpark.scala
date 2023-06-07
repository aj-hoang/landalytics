package com.landalytics.utilities.sparkhelpers

import org.apache.spark.sql.{SaveMode, SparkSession}

abstract class TypedSpark {

  def run(spark: SparkSession, sourceUri: String, destinationUri: String): Unit

}
