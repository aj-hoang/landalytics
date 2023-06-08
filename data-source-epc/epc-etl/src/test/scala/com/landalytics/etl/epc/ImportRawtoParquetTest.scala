package com.landalytics.etl.epc

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ImportRawtoParquetTest extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  it should "Import raw epc to parquet" in {
    val baseUri = "/ImportRawToParquet/epc"
    val sourceUri = getClass.getResource(s"$baseUri/csv/epc.csv").getPath
    val destinationUri = getClass.getResource(s"$baseUri").getPath ++ "/raw/epc.parquet"
    println(sourceUri)
    println(destinationUri)

    // Should have no runtime errors
    ImportRawToParquet.run(spark, sourceUri, destinationUri)

    spark.read.parquet(destinationUri).printSchema()
    spark.read.parquet(destinationUri).show(false)

    succeed
  }

}
