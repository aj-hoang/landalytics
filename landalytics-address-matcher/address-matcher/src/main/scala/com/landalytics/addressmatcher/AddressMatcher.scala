package com.landalytics.addressmatcher

import com.landalytics.addressmatcher.ConfigModel.AddressMatcherConfig
import com.landalytics.model.addresscore.AddressCoreModel.AddressCore
import com.landalytics.utilities.addressparsers.ParsedAddressModel.ParsedAddress
import com.landalytics.utilities.sparkhelpers.ExtraConfigSparkRunner
import org.apache.spark.sql.{Column, Dataset, SaveMode, SparkSession}
import io.circe.generic.auto._
import org.apache.spark.sql.functions.{col, expr}

object AddressMatcher extends ExtraConfigSparkRunner[AddressMatcherConfig] {

  case class SourceClass(id: String, parsedAddress: ParsedAddress)
  case class JoinedClass(sourceClass: SourceClass, addressCore: AddressCore)

  def run(spark: SparkSession, config: AddressMatcherConfig): Unit = {
    import spark.implicits._

    def addressMatch(
                      sourceDS: Dataset[SourceClass],
                      addressCoreDS: Dataset[AddressCore],
                      joinCondition: Column
                    ): (Dataset[JoinedClass], Dataset[SourceClass]) = {

      val joinedDS = sourceDS.as("sourceDS").joinWith(addressCoreDS.as("addressCoreDS"), joinCondition, "left").map{
        case (sourceClass, addressCore) => JoinedClass(sourceClass, addressCore)
      }

      val matchedDS = joinedDS.where("addressCore.id is not null")
      val notMatchedDS = joinedDS.where("addressCore.id is null").map(_.sourceClass)

      joinedDS.map(_.addressCore)

      (matchedDS, notMatchedDS)
    }

    // Match a datasource to the address core
    val sourceUri: String = config.sourceUri
    val sourceIdField: String = config.sourceIdField
    val addressCoreUri: String = config.addressCoreUri
    val addressMatchedUri: String = config.addressMatchedUri
    val addressNotMatchedUri: String = config.addressNotMatchedUri

    // If data in sourceUri contains parsedAddress, apply matching for the parsedAddress fields
    val sourceDS = spark.read.parquet(sourceUri)
      .select(col(sourceIdField).as("id"), col("parsedAddress"))
      .as[SourceClass]

    val addressCoreDS = spark.read.parquet(addressCoreUri).as[AddressCore]

    val joinCondition1 =
      """
        |sourceDS.parsedAddress.postcode = addressCoreDS.parsedAddress.postcode
        | AND sourceDS.parsedAddress.houseNumber = addressCoreDS.parsedAddress.houseNumber
        | AND sourceDS.parsedAddress.street = addressCoreDS.parsedAddress.street
        | AND sourceDS.parsedAddress.town = addressCoreDS.parsedAddress.town
        | AND sourceDS.parsedAddress.city = addressCoreDS.parsedAddress.city
        | AND sourceDS.parsedAddress.flat is null
        | AND addressCoreDS.parsedAddress.flat is null
        |""".stripMargin


    val joinCondition2 =
      """
        |sourceDS.parsedAddress.postcode = addressCoreDS.parsedAddress.postcode
        | AND sourceDS.parsedAddress.houseNumber = addressCoreDS.parsedAddress.houseNumber
        | AND sourceDS.parsedAddress.street = addressCoreDS.parsedAddress.street
        | AND sourceDS.parsedAddress.city = addressCoreDS.parsedAddress.city
        | AND sourceDS.parsedAddress.flat is null
        | AND addressCoreDS.parsedAddress.flat is null
        |""".stripMargin

    val joinCondition3 =
      """
        |sourceDS.parsedAddress.postcode = addressCoreDS.parsedAddress.postcode
        | AND sourceDS.parsedAddress.flat = addressCoreDS.parsedAddress.flat
        | AND sourceDS.parsedAddress.houseNumber = addressCoreDS.parsedAddress.houseNumber
        | AND sourceDS.parsedAddress.street = addressCoreDS.parsedAddress.street
        | AND sourceDS.parsedAddress.town = addressCoreDS.parsedAddress.town
        | AND sourceDS.parsedAddress.city = addressCoreDS.parsedAddress.city
        |""".stripMargin

    val joinCondition4 =
      """
        |sourceDS.parsedAddress.postcode = addressCoreDS.parsedAddress.postcode
        | AND sourceDS.parsedAddress.flat = addressCoreDS.parsedAddress.flat
        | AND sourceDS.parsedAddress.houseNumber = addressCoreDS.parsedAddress.houseNumber
        | AND sourceDS.parsedAddress.street = addressCoreDS.parsedAddress.street
        | AND sourceDS.parsedAddress.city = addressCoreDS.parsedAddress.city
        |""".stripMargin


    val joinConditions = Seq(joinCondition1, joinCondition2, joinCondition3, joinCondition4)

    val (combinedMatchedDS, combinedNotMatchedDS) = joinConditions.foldLeft((spark.emptyDataset[JoinedClass], sourceDS)){ (accum, jc) =>
      val (matchedDS, notMatchedDS) = addressMatch(accum._2, addressCoreDS, expr(jc))
      (accum._1 unionAll matchedDS, notMatchedDS)
    }


    // Write matched results
    combinedMatchedDS.write
      .mode(SaveMode.Overwrite)
      .parquet(addressMatchedUri)

    // Write unmatched results
    combinedNotMatchedDS.write
      .mode(SaveMode.Overwrite)
      .parquet(addressNotMatchedUri)


  }

}
