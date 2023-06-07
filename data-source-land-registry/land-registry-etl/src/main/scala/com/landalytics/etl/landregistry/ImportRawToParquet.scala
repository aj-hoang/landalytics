package com.landalytics.etl.landregistry

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.landalytics.model.landregistry.raw.RawLandRegistryModel.RawLandRegistry
import com.landalytics.utilities.sparkhelpers.SparkRunner
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object ImportRawToParquet extends SparkRunner {
  def run(spark: SparkSession, sourceUri: String, destinationUri: String): Unit = {
    import spark.implicits._

    // Land registry data does not contain header, so we extract schema from the case class to use when reading in the csv
    val schema = ScalaReflection.schemaFor[RawLandRegistry].dataType.asInstanceOf[StructType]

    spark.read
      .option("header", "false")
      .schema(schema)
      .csv(sourceUri)
      .as[RawLandRegistry]
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationUri)
  }
}
