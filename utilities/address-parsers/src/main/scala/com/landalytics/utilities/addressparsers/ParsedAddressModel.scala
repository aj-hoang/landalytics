package com.landalytics.utilities.addressparsers

import scala.reflect.ClassTag
import com.mapzen.jpostal.{AddressParser, ParsedComponent}

object ParsedAddressModel {

//  def capitalizeWordsInString(s: String) = s.split(" ").map(_.capitalize).mkString(" ")

  def cleanseFullAddress(address: String): String = {
    address
      .replaceAll("\\s{2,}", " ")
      .replaceAll("\\s?,\\s", ", ")
      .toUpperCase
      .trim
  }

  def parseAddress(address: String): Seq[AddressPart] = {
    val addressParser: AddressParser = AddressParser.getInstance()
    val parsedAddress = addressParser.parseAddress(address)

//    parsedAddress.foreach( c =>
//      println(s"label: ${c.getLabel} value: ${c.getValue}")
//    )

    def libpostalAddressComponentToAddressPart(addressComponent: ParsedComponent): Option[AddressPart] = {
      val capitalizedValue = addressComponent.getValue.split(" ").map(_.capitalize).mkString(" ")
      lazy val upperValue = addressComponent.getValue.toUpperCase

      addressComponent.getLabel match {
        case "house_number" => Some(HouseNumber(capitalizedValue))
        case "unit" | "house" => Some(Flat(capitalizedValue))
        case "road" => Some(Street(capitalizedValue))
        case "city_district" => Some(Town(capitalizedValue))
        case "city" => Some(City(capitalizedValue))
        case "postcode" => Some(Postcode(upperValue))
        case "country" => Some(Country(capitalizedValue))
        case _ => None
      }
    }

    parsedAddress.flatMap(libpostalAddressComponentToAddressPart)
  }

  sealed trait AddressPart {
    val value: String
  }

  case class HouseNumber(value: String) extends AddressPart
  case class Flat(value: String) extends AddressPart
  case class Street(value: String) extends AddressPart
  case class Town(value: String) extends AddressPart
  case class City(value: String) extends AddressPart
  case class Postcode(value: String) extends AddressPart
  case class Country(value: String) extends AddressPart

  case class ParsedAddress(
                           fullAddress: String,
                           cleansedfullAddress: String,
                           houseNumber: Option[String],
                           flat: Option[String],
                           street: Option[String],
                           town: Option[String],
                           city: Option[String],
                           postcode: Option[String],
                           country: Option[String]
                           )

  object ParsedAddress {

    def apply(fullAddress: String, knownAddressParts: Seq[AddressPart]): ParsedAddress = {

      def getAddressPart[T <: AddressPart: ClassTag](knownAddressParts: Seq[AddressPart]): Option[String] = {
        knownAddressParts.flatMap {
          case x: T => Some(x.value)
          case _ => None
        }.headOption
      }

      lazy val parsedFullAddress = parseAddress(fullAddress)
      lazy val combinedParsedAddress = knownAddressParts ++ parsedFullAddress

      // Return prepopulated values for House number, Flat etc.. if supplied
      this(
        fullAddress,
        cleanseFullAddress(fullAddress),
        getAddressPart[HouseNumber](combinedParsedAddress),
        getAddressPart[Flat](combinedParsedAddress),
        getAddressPart[Street](combinedParsedAddress),
        getAddressPart[Town](combinedParsedAddress),
        getAddressPart[City](combinedParsedAddress),
        getAddressPart[Postcode](combinedParsedAddress),
        getAddressPart[Country](combinedParsedAddress)
      )
    }
  }
}
