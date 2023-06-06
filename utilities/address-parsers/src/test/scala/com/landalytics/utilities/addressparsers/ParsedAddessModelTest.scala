package com.landalytics.utilities.addressparsers

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.landalytics.utilities.addressparsers.ParsedAddressModel._

class ParsedAddessModelTest extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  it should "parse address in case class" in {

    val parsedAddress = List(
      ParsedAddress(
        fullAddress = "2 Foobar Lane, London, P05TC0DE",
        Seq(HouseNumber("2"), Street("Foobar Lane"), City("London"), Country("UK"))
      ),
      ParsedAddress(
        fullAddress = "40 SELSDON ROAD, CROYDON, LONDON, CR2 6PB",
        Seq(Country("UK"))
      ),
      ParsedAddress(
        fullAddress = "40, SELSDON ROAD, CROYDON, LONDON, CR2 6PB",
        Seq(Country("UK"))
      ),
      ParsedAddress(
        fullAddress = "FLAT 1, 11 SHEEN PARK, RICHMOND, TW9 1UN",
        Seq(Country("UK"))
      ),
      ParsedAddress(
        fullAddress = "11, FLAT 1, SHEEN PARK, RICHMOND, TW9 1UN",
        Seq(Country("UK"))
      ),
      ParsedAddress(
        fullAddress = "5 WORDSWORTH COURT, 6 LOVELACE ROAD, SURBITON, KINGSTON UPON THAMES, LONDON, KT6 6PD",
        Seq(Country("UK"))
      ),
      ParsedAddress(
        fullAddress = "WORDSWORTH COURT, 5, 6, LOVELACE ROAD, SURBITON, KINGSTON UPON THAMES, LONDON, KT6 6PD",
        Seq(Country("UK"))
      ),
      ParsedAddress(
        fullAddress = "BROOKLYN COURT, 476, FLAT 1, CHRISTCHURCH ROAD, Bournemouth, BH1 4BD",
        Seq(Country("UK"))
      ),
      ParsedAddress(
        fullAddress = "BOWLINE COURT, FLAT 7, TRINITY WAY,MINEHEAD,TA24 6GP",
        Seq(Country("UK"))
      )
    ).toDS

    parsedAddress.printSchema()
    parsedAddress.show(false)

    // No runtime errors expected
    succeed

  }

  it should "use libpostal to parse address" in {

    parseAddress("40 SELSDON ROAD, CROYDON, LONDON, CR2 6PB, UK")

    println("another address")

    parseAddress("FLAT 1, 11 SHEEN PARK, RICHMOND, TW9 1UN, UK")

    println("another address!!")

    parseAddress("5 WORDSWORTH COURT, 6 LOVELACE ROAD, SURBITON, KINGSTON UPON THAMES, LONDON, KT6 6PD")

    // no runtime errors
    succeed
  }

  it should "cleanse full address" in {
    val actual = cleanseFullAddress("FLAT 1             , 11        SHeeN PARK,     RIchmOND, TW9 1UN")
    val expected = "FLAT 1, 11 SHEEN PARK, RICHMOND, TW9 1UN"

    actual shouldEqual expected
  }

}
