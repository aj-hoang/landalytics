package com.landalytics.etl.codepointopen

import com.landalytics.model.codepointopen.raw.RawCodepointOpenModel.RawCodepointOpen
import com.landalytics.utilities.sparkhelpers.SparkRunner
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}

object ImportRawToParquet extends SparkRunner {

  def run(spark: SparkSession, sourceUri: String, destinationUri: String): Unit = {
    import spark.implicits._

    // Codepoint Open data does not contain header, so we extract schema from the case class to use when reading in the csv
    val schema = ScalaReflection.schemaFor[RawCodepointOpen].dataType.asInstanceOf[StructType]

    spark.read
      .option("header", "false")
      .schema(schema)
      .csv(sourceUri)
      .as[RawCodepointOpen]
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destinationUri)

  }

}
