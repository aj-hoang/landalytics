package com.landalytics.etl.epc

import com.landalytics.model.epc.clean.CleanEPCModel._
import com.landalytics.model.epc.raw.RawEPCModel.RawEPC
import com.landalytics.utilities.addressparsers.ParsedAddressModel.{AddressPart, Country, ParsedAddress, Postcode}
import com.landalytics.utilities.sparkhelpers.SparkRunner
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Date

object CreateCaseClassSlim extends SparkRunner {

  def run(spark: SparkSession, sourceUri: String, destinationUri: String): Unit = {
    import spark.implicits._

    def convertDateStringToDate(s: String): Date = Date.valueOf(s)

    val rawEpcDS = spark.read.parquet(sourceUri).as[RawEPC]

    val epcDS = rawEpcDS.map{ rawEpc =>
      val fullAddress = Seq(rawEpc.ADDRESS, rawEpc.POSTTOWN, rawEpc.POSTCODE).flatten.mkString(", ")
      val knownAddressParts: Seq[AddressPart] = Seq(
        rawEpc.POSTCODE.map(Postcode),
        Some(Country("UK"))
      ).flatten

      EPCSlim(
        lmkKey = rawEpc.LMK_KEY,
        fullAddress = fullAddress,
        parsedAddress = ParsedAddress(fullAddress, knownAddressParts),
        uprn = rawEpc.UPRN,
        uprnSource = rawEpc.UPRN_SOURCE
      )

    }

    epcDS.write
      .mode(SaveMode.Overwrite)
      .parquet(destinationUri)

  }

}
