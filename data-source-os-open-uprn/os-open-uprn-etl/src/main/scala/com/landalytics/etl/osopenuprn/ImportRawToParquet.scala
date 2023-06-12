package com.landalytics.etl.osopenuprn

import com.landalytics.model.osopenuprn.raw.RawOsOpenUprnModel.RawOsOpenUprn
import com.landalytics.utilities.confighelpers.ConfigModels.OsOpenUprnConfig
import com.landalytics.utilities.sparkhelpers.ExtraConfigSparkRunner
import io.circe.generic.auto._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ImportRawToParquet extends ExtraConfigSparkRunner[OsOpenUprnConfig] {
  def run(spark: SparkSession, config: OsOpenUprnConfig): Unit = {

    import spark.implicits._

    val sourceUri = config.importRawToParquetSourceUri
    val destinationUri = config.importRawToParquetDestinationUri

    spark.read
      .option("header", "true")
      .csv(sourceUri)
      .as[RawOsOpenUprn]
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationUri)

  }
}
