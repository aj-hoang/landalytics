package com.landalytics.etl.landregistry

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ImportRawtoParquetTest extends AnyFlatSpec with Matchers {

  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  it should "import raw to parquet" in {
    val sourceUri = getClass.getResource("/land-registry/csv/land-registry.csv").getPath
    val destinationUri = getClass.getResource("/land-registry").getPath ++ "/raw/land-registry.parquet"
    println(sourceUri)
    println(destinationUri)

    // Should have no runtime errors
    ImportRawToParquet.run(spark, sourceUri, destinationUri)

    spark.read.parquet(destinationUri).printSchema()
    spark.read.parquet(destinationUri).show(false)

    succeed
  }

}
