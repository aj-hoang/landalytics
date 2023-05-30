package com.landalytics.etl.landregistry

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.landalytics.model.landregistry.raw.RawLandRegistryModel.RawLandRegistry

object ImportRawToParquet {
  def run(spark: SparkSession, sourceUri: String, destinationUri: String, saveMode: SaveMode = SaveMode.Overwrite) = {
    import spark.implicits._

    spark.read
      .option("header", "false")
      .csv(sourceUri)
      .as[RawLandRegistry]
      .write
      .mode(saveMode)
      .parquet(destinationUri)
  }
}
