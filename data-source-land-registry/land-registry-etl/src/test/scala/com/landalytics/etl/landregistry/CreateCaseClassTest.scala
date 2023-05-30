package com.landalytics.etl.landregistry

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CreateCaseClassTest extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  it should "create the case class" in {
    val baseUri = "/CreateCaseClass/land-registry"
    val sourceUri = getClass.getResource(s"$baseUri/raw/land-registry.parquet").getPath
    val destinationUri = getClass.getResource(s"$baseUri").getPath ++ "/cleanModel/land-registry.parquet"
    println(sourceUri)
    println(destinationUri)

    CreateCaseClass.run(spark, sourceUri, destinationUri)

    spark.read.parquet(destinationUri).printSchema()
    spark.read.parquet(destinationUri).show(false)

    // should have no runtime errors
    succeed

  }

}
