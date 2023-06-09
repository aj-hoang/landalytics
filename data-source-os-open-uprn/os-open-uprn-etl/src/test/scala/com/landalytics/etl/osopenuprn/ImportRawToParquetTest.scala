package com.landalytics.etl.osopenuprn

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ImportRawToParquetTest extends AnyFlatSpec with Matchers {

  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  it should "import raw to parquet for OS open Uprn" in {

    val baseUri = "/ImportRawToParquet/os-open-uprn"
    val sourceUri = getClass.getResource(s"$baseUri/csv/os-open-uprn.csv").getPath
    val destinationUri = getClass.getResource(s"$baseUri").getPath ++ "/raw/os-open-uprn.parquet"
    println(sourceUri)
    println(destinationUri)

    // Should have no runtime errors
    ImportRawToParquet.run(spark, sourceUri, destinationUri)

    spark.read.parquet(destinationUri).printSchema()
    spark.read.parquet(destinationUri).show(false)

    succeed
  }

}
