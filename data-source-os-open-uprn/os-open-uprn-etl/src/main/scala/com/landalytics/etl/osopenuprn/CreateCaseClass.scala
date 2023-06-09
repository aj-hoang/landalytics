package com.landalytics.etl.osopenuprn

import com.landalytics.model.osopenuprn.raw.RawOsOpenUprnModel.RawOsOpenUprn
import com.landalytics.model.osopenuprn.clean.CleanOsOpenUprnModel.OsOpenUprn
import com.landalytics.utilities.sparkhelpers.SparkRunner
import org.apache.spark.sql.{SaveMode, SparkSession}

object CreateCaseClass extends SparkRunner {
  def run(spark: SparkSession, sourceUri: String, destinationUri: String): Unit = {
    import spark.implicits._

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
