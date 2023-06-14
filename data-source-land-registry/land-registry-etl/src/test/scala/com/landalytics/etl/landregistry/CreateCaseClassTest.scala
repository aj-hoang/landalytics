package com.landalytics.etl.landregistry

import com.landalytics.model.landregistry.raw.RawLandRegistryModel.RawLandRegistry
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CreateCaseClassTest extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  it should "create the case class" in {

    // Write sample RawLandRegistry to parquet
    val sampleRawDS = List(RawLandRegistry(
      transactionUniqueId = "someId",
      price = Some("12345"),
      dateOfTransfer = Some("2023-01-01 00:00"),
      postcode = Some("CRPOSTCODE"),
      propertyType = None,
      oldNew = None,
      duration = None,
      paon = Some("20"),
      saon = None,
      street = Some("foobar street"),
      locality = None,
      townCity = None,
      district = None,
      county = None,
      ppdCategoryType = None,
      recordStatus = None
    )).toDS

    val baseUri = getClass.getResource("/CreateCaseClass/land-registry").getPath
    val sampleRawLandRegistryUri = baseUri ++ "/raw/land-registry.parquet"
    sampleRawDS.write
      .mode(SaveMode.Overwrite)
      .parquet(sampleRawLandRegistryUri)

    // Use above created parquet in createCaseClass call
    val sourceUri = sampleRawLandRegistryUri
    val destinationUri = baseUri ++ "/cleanModel/land-registry.parquet"
    println(sourceUri)
    println(destinationUri)

    CreateCaseClass.run(spark, sourceUri, destinationUri)

    spark.read.parquet(destinationUri).printSchema()
    spark.read.parquet(destinationUri).show(false)

    // should have no runtime errors
    succeed

  }

}
