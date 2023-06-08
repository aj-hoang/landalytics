package com.landalytics.etl.epc

import com.landalytics.model.epc.raw.RawEPCModel.RawEPC
import com.landalytics.utilities.sparkhelpers.SparkRunner
import org.apache.spark.sql.{SaveMode, SparkSession}

object ImportRawToParquet extends SparkRunner {

  def run(spark: SparkSession, sourceUri: String, destinationUri: String): Unit = {
    import spark.implicits._

    spark.read
      .option("header", "true")
      .csv(sourceUri)
      .as[RawEPC]
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationUri)
  }

}
