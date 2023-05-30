package com.landalytics.etl.landregistry

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.landalytics.model.landregistry.raw.RawLandRegistryModel.RawLandRegistry
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object ImportRawToParquet {
  def run(spark: SparkSession, sourceUri: String, destinationUri: String, saveMode: SaveMode = SaveMode.Overwrite) = {
    import spark.implicits._

    // Land registry data does not contain header, so we extract schema from the case class to use when reading in the csv
    val schema = ScalaReflection.schemaFor[RawLandRegistry].dataType.asInstanceOf[StructType]

    spark.read
      .option("header", "false")
      .schema(schema)
      .csv(sourceUri)
      .as[RawLandRegistry]
      .write
      .mode(saveMode)
      .parquet(destinationUri)
  }
}
