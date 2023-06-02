package com.landalytics.utilities.addressparsers

import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.landalytics.utilities.addressparsers.ParsedAddressModel._

class ParsedAddessModelTest extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  it should "parse address" in {

    val parsedAddress = List(ParsedAddress(
      fullAddress = "2 Foobar Lane, London, P05TC0DE",
      Seq(Paon("2"), Street("Foobar Lane"), TownCity("London"), Country("GBR"))
    )).toDS

    parsedAddress.printSchema()
    parsedAddress.show(false)

  }

}
