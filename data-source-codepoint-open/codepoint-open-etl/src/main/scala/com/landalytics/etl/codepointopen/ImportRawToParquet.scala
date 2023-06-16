package com.landalytics.etl.codepointopen

import com.landalytics.model.codepointopen.config.ConfigModel.CodepointOpenConfig
import com.landalytics.model.codepointopen.raw.RawCodepointOpenModel.RawCodepointOpen
import com.landalytics.utilities.sparkhelpers.ExtraConfigSparkRunner
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SaveMode, SparkSession}
import io.circe.generic.auto._

object ImportRawToParquet extends ExtraConfigSparkRunner[CodepointOpenConfig] {

  def run(spark: SparkSession, config: CodepointOpenConfig): Unit = {
    import spark.implicits._

    // Codepoint Open data does not contain header, so we extract schema from the case class to use when reading in the csv
    val schema = ScalaReflection.schemaFor[RawCodepointOpen].dataType.asInstanceOf[StructType]

    spark.read
      .option("header", "false")
      .schema(schema)
      .csv(config.csvUri)
      .as[RawCodepointOpen]
      .write
      .mode(SaveMode.Overwrite)
      .parquet(config.rawParquetUri)

  }

}
