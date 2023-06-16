package com.landalytics.etl.codepointopen

import com.landalytics.model.codepointopen.config.ConfigModel.CodepointOpenConfig
import com.landalytics.model.codepointopen.raw.RawCodepointOpenModel.RawCodepointOpen
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CreateCaseClassTest extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local")
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
    .getOrCreate()

  SedonaSQLRegistrator.registerAll(spark)
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  it should "create case class for codepoint open" in {

    val rawCodepointOpenDS = Seq(
      RawCodepointOpen(
        postcode = "POSTCODE",
        positionalQualityIndicator = "10",
        eastings = "539052",
        northings = "162028",
        countryCode = "",
        nhsRegionalHACode = "",
        nhsHACode = "",
        adminCountyCode = "",
        adminDistrictCode = "",
        adminWardCode = ""
      )
    ).toDS

    val baseUri = getClass.getResource("/CreateCaseClass/codepoint-open").getPath
    val sampleRawCodepointOpenUri = baseUri ++ "/raw/codepoint-open.parquet"
    rawCodepointOpenDS.write
      .mode(SaveMode.Overwrite)
      .parquet(sampleRawCodepointOpenUri)

    val caseClassParquetUri = baseUri ++ "/clean/codepoint-open.parquet"

    val codepointOpenConfig = CodepointOpenConfig(
      csvUri = "",
      rawParquetUri = sampleRawCodepointOpenUri,
      caseClassParquetUri = caseClassParquetUri
    )

    CreateCaseClass.run(spark, codepointOpenConfig)

    spark.read.parquet(caseClassParquetUri).printSchema()
    spark.read.parquet(caseClassParquetUri).show(false)

    // should have no runtime errors
    succeed


  }

}
