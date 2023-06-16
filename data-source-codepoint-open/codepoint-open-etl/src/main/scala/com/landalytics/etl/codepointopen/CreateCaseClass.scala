package com.landalytics.etl.codepointopen

import com.landalytics.model.codepointopen.clean.CleanCodepointOpenModel.CodepointOpen
import com.landalytics.model.codepointopen.config.ConfigModel.CodepointOpenConfig
import com.landalytics.model.codepointopen.raw.RawCodepointOpenModel.RawCodepointOpen
import com.landalytics.utilities.sparkhelpers.{ExtraConfigSparkRunner, SparkRunner}
import org.apache.spark.sql.{SaveMode, SparkSession}
import io.circe.generic.auto._

object CreateCaseClass extends ExtraConfigSparkRunner[CodepointOpenConfig] {
  override def useSedona = true
  def run(spark: SparkSession, config: CodepointOpenConfig): Unit = {
    import spark.implicits._

    val sourceUri = config.rawParquetUri
    val destinationUri = config.caseClassParquetUri

    val rawCodepointOpenDS = spark.read.parquet(sourceUri).as[RawCodepointOpen]
    rawCodepointOpenDS.createOrReplaceTempView("raw_codepoint_open")

    val cleanCodepointDS = spark.sql(
      """
        |SELECT
        |  postcode,
        |  ST_Y(geometry) as latitude,
        |  ST_X(geometry) as longitude
        |FROM (
        |    SELECT postcode,
        |       ST_TRANSFORM(ST_Point(eastings, northings), 'epsg:27700', 'epsg:4326') as geometry
        |    FROM raw_codepoint_open
        |) rco
        |""".stripMargin).as[CodepointOpen]

    cleanCodepointDS.write
      .mode(SaveMode.Overwrite)
      .parquet(destinationUri)

  }
}
