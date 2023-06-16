package com.landalytics.addressmatcher

import com.landalytics.model.addresscore.AddressCoreModel
import com.landalytics.model.addresscore.AddressCoreModel.AddressCore
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.landalytics.model.landregistry.clean.CleanLandRegistryModel._
import com.landalytics.utilities.addressparsers.ParsedAddressModel.ParsedAddress

import java.sql.Date

class AddressMatcherTest extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  it should "do some address matching" in {

    val baseUri = getClass.getResource("/AddressMatcher").getPath
    val sourceUri = baseUri ++ "/Source/land-registry.parquet"
    val addressCoreUri = baseUri ++ "/AddressCore/address-core.parquet"
    val addressMatchedUri = baseUri ++ "/generated/address-matched.parquet"
    val addressNotMatchedUri = baseUri ++ "/generated/address-not-matched.parquet"

    val addressMatcherConfig = ConfigModel.AddressMatcherConfig(
      sourceUri = sourceUri,
      sourceIdField = "transactionUniqueId",
      addressCoreUri = addressCoreUri,
      addressMatchedUri = addressMatchedUri,
      addressNotMatchedUri = addressNotMatchedUri
    )

    // sample parsed Address
    val parsedAddress = ParsedAddress(
      fullAddress = "63, HOMEFIELD GARDENS, MITCHAM, MERTON, CR4 3BZ",
      cleansedfullAddress = "63, HOMEFIELD GARDENS, MITCHAM, MERTON, CR4 3BZ",
      houseNumber = Some("63"),
      flat = None,
      street = Some("HOMEFIELD GARDENS"),
      town = Some("MERTON"),
      city = Some("MITCHAM"),
      postcode = Some("CR4 3BZ"),
      sectorCode = Some("CR4 3"),
      districtCode = Some("CR4"),
      areaCode = Some("CR"),
      country = Some("UK")
    )

    val parsedAddress2 = ParsedAddress(
      fullAddress = "64, HOMEFIELD GARDENS, MITCHAM, MERTON, CR4 3BZ",
      cleansedfullAddress = "64, HOMEFIELD GARDENS, MITCHAM, MERTON, CR4 3BZ",
      houseNumber = Some("64"),
      flat = None,
      street = Some("HOMEFIELD GARDENS"),
      town = Some("MERTON"),
      city = Some("MITCHAM"),
      postcode = Some("CR4 3BZ"),
      sectorCode = Some("CR4 3"),
      districtCode = Some("CR4"),
      areaCode = Some("CR"),
      country = Some("UK")
    )


    // write sample land registry parquet
    Seq(
      LandRegistry(
        transactionUniqueId = "SomeID",
        landRegistryTransaction = LandRegistryTransaction(
          transactionUniqueId = "SomeID",
          price = Some(300000d),
          dateOfTransfer = Some(Date.valueOf("2023-01-01")),
          ppdCategoryType = Some("A"),
          propertyType = Some("T"),
          oldNew = Some("Old"),
          duration = Some("F")
        ),
        fullAddress = "63, HOMEFIELD GARDENS, MITCHAM, MERTON, CR4 3BZ",
        parsedAddress = parsedAddress
      ),
      LandRegistry(
        transactionUniqueId = "SomeID2",
        landRegistryTransaction = LandRegistryTransaction(
          transactionUniqueId = "SomeID2",
          price = Some(300000d),
          dateOfTransfer = Some(Date.valueOf("2023-01-01")),
          ppdCategoryType = Some("A"),
          propertyType = Some("T"),
          oldNew = Some("Old"),
          duration = Some("F")
        ),
        fullAddress = "64, HOMEFIELD GARDENS, MITCHAM, MERTON, CR4 3BZ",
        parsedAddress = parsedAddress2
      )
    ).toDS.write
      .mode(SaveMode.Overwrite)
      .parquet(sourceUri)
    
    // write sample address core parquet
    Seq(AddressCore(
      id = "AddressCoreId", 
      geometry = AddressCoreModel.AddressGeometry(latitude = 51.3981405d, longitude = -0.0675292d),
      geometrySourceId = "GeometrySourceId",
      geometrySourceType = "GeometrySourceType",
      fullAddress = Some("63, HOMEFIELD GARDENS, MITCHAM, MERTON, CR4 3BZ"),
      parsedAddress = Some(parsedAddress),
      addressSourceId = Some("AddressID"),
      addressSourceType = Some("AddressSource")
    )).toDS.write
      .mode(SaveMode.Overwrite)
      .parquet(addressCoreUri)

    // no runtime errors
    AddressMatcher.run(spark, addressMatcherConfig)

    spark.read.parquet(addressMatchedUri).printSchema()
    spark.read.parquet(addressMatchedUri).show(false)

    spark.read.parquet(addressNotMatchedUri).printSchema()
    spark.read.parquet(addressNotMatchedUri).show(false)

    succeed

  }


}
