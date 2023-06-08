package com.landalytics.etl.landregistry

import com.landalytics.etl.landregistry.LandRegistryUtils.constructFullAddress
import com.landalytics.model.landregistry.raw.RawLandRegistryModel.RawLandRegistry
import com.landalytics.model.landregistry.clean.CleanLandRegistryModel.{LandRegistry, LandRegistryTransaction}
import com.landalytics.utilities.addressparsers.ParsedAddressModel.{AddressPart, City, ParsedAddress, Postcode, Street, Town, Country, cleanseStringSimple}
import com.landalytics.utilities.sparkhelpers.SparkRunner
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Date

object CreateCaseClass extends SparkRunner {

  private def convertLrDateOfTransferStringToDate(dateString: String): Date = {
    Date.valueOf(dateString.substring(0,10))
  }

  def run(spark: SparkSession, sourceUri: String, destinationUri: String) = {
    import spark.implicits._

    val rawLrDS = spark.read.parquet(sourceUri).as[RawLandRegistry]

    val lrDS = rawLrDS.map{ rawLr =>
      val fullAddress = cleanseStringSimple(constructFullAddress(Seq(rawLr.paon, rawLr.saon, rawLr.street, rawLr.townCity, rawLr.district, rawLr.postcode)))

      val knownAddressParts: Seq[AddressPart] = Seq(
        rawLr.street.map(Street),
        rawLr.townCity.map(City),
        rawLr.district.map(Town),
        rawLr.postcode.map(Postcode),
        Some(Country("UK"))
      ).flatten

      LandRegistry(
        transactionUniqueId = rawLr.transactionUniqueId,
        fullAddress = fullAddress,
        parsedAddress = ParsedAddress(fullAddress, knownAddressParts),
        landRegistryTransaction = LandRegistryTransaction(
          transactionUniqueId = rawLr.transactionUniqueId,
          price = rawLr.price.map(_.toDouble),
          dateOfTransfer = rawLr.dateOfTransfer.map(convertLrDateOfTransferStringToDate),
          ppdCategoryType = rawLr.ppdCategoryType,
          propertyType = rawLr.propertyType,
          oldNew = rawLr.oldNew,
          duration = rawLr.duration
        )
      )
    }

    lrDS.write
      .mode(SaveMode.Overwrite)
      .parquet(destinationUri)
  }

}
