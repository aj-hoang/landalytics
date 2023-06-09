package com.landalytics.etl.osopenuprn

import com.landalytics.model.osopenuprn.raw.RawOsOpenUprnModel.RawOsOpenUprn
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CreateCaseClassTest extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  it should "Create the case class for os open uprn" in {

    val sampleRawDS = List(RawOsOpenUprn(
      UPRN = "2222",
      X_COORDINATE = "123456",
      Y_COORDINATE = "3458273.5",
      LATITUDE = "51.5065027",
      LONGITUDE = "-2.6975704"
    )).toDS

    val baseUri = getClass.getResource("/CreateCaseClass/os-open-uprn").getPath
    val sampleRawOsOpenUprnUri = baseUri ++ "/raw/os-open-uprn.parquet"
    sampleRawDS.write
      .mode(SaveMode.Overwrite)
      .parquet(sampleRawOsOpenUprnUri)

    // Use above created parquet in createCaseClass call
    val sourceUri = sampleRawOsOpenUprnUri
    val destinationUri = baseUri ++ "/cleanModel/os-open-uprn.parquet"
    println(sourceUri)
    println(destinationUri)

    CreateCaseClass.run(spark, sourceUri, destinationUri)

    spark.read.parquet(destinationUri).printSchema()
    spark.read.parquet(destinationUri).show(false)

    // should have no runtime errors
    succeed


  }

}
