package com.landalytics.etl.osopenuprn

import com.landalytics.model.osopenuprn.raw.RawOsOpenUprnModel.RawOsOpenUprn
import com.landalytics.model.osopenuprn.clean.CleanOsOpenUprnModel.OsOpenUprn
import com.landalytics.model.osopenuprn.ConfigModel.OsOpenUprnConfig
import com.landalytics.utilities.sparkhelpers.ExtraConfigSparkRunner
import org.apache.spark.sql.{SaveMode, SparkSession}
import io.circe.generic.auto._

object CreateCaseClass extends ExtraConfigSparkRunner[OsOpenUprnConfig] {
  def run(spark: SparkSession, config: OsOpenUprnConfig): Unit = {
    import spark.implicits._

    val sourceUri = config.rawParquetUri
    val destinationUri = config.createCaseClassParquetUri

    val rawOsOpenUprnDS = spark.read.parquet(sourceUri).as[RawOsOpenUprn]

    val osOpenUprnDS = rawOsOpenUprnDS.map{ rawDS =>
      OsOpenUprn(
        uprn = rawDS.UPRN,
        xCoordinate = rawDS.X_COORDINATE.toDouble,
        yCoordinate = rawDS.Y_COORDINATE.toDouble,
        latitude = rawDS.LATITUDE.toDouble,
        longitude = rawDS.LONGITUDE.toDouble
      )
    }

    osOpenUprnDS.write
      .mode(SaveMode.Overwrite)
      .parquet(destinationUri)

  }
}
