package com.landalytics.etl.osopenuprn

import com.landalytics.model.osopenuprn.raw.RawOsOpenUprnModel.RawOsOpenUprn
import com.landalytics.utilities.sparkhelpers.SparkRunner
import org.apache.spark.sql.{SaveMode, SparkSession}

object ImportRawToParquet extends SparkRunner {
  def run(spark: SparkSession, sourceUri: String, destinationUri: String): Unit = {

    import spark.implicits._

    spark.read
      .option("header", "true")
      .csv(sourceUri)
      .as[RawOsOpenUprn]
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationUri)

  }
}
