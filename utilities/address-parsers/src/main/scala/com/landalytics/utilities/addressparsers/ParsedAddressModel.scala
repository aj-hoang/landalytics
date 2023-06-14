package com.landalytics.utilities.addressparsers

import scala.reflect.ClassTag
import com.mapzen.jpostal.{AddressParser, ParsedComponent}

object ParsedAddressModel {

  def cleanseStringSimple(s: String): String = {
    // Simple cleanse of multiple whitespaces and trimming
    s.replaceAll("\\s{2,}", " ")
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
//      val capitalizedValue = addressComponent.getValue.split(" ").map(_.capitalize).mkString(" ")
      val value = addressComponent.getValue

      addressComponent.getLabel match {
        case "house_number" => Some(HouseNumber(value))
        case "unit" | "house" => Some(Flat(value))
        case "road" => Some(Street(value))
        case "city_district" => Some(Town(value))
        case "city" => Some(City(value))
        case "postcode" => Some(Postcode(value))
        case "country" => Some(Country(value))
        case _ => None
      }
    }

    parsedAddress.flatMap(libpostalAddressComponentToAddressPart)
  }

  def parsePostcode(postcode: Option[String]): Seq[AddressPart] = {

    // regex string to extract Sector, district and area codes from postcode
    val regexString = """((([A-Za-z]{1,2})\d{1}[A-Za-z\d]?)\s{1,2}\d{1}).*""".r

    postcode match {
      case Some(x) => x match {
        case regexString(sector, district, area) => Seq(SectorCode(sector), DistrictCode(district), AreaCode(area))
        case _ => Seq()
      }
      case _ => Seq()
    }

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
  case class SectorCode(value: String) extends AddressPart
  case class DistrictCode(value: String) extends AddressPart
  case class AreaCode(value: String) extends AddressPart
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
                           sectorCode: Option[String],
                           districtCode: Option[String],
                           areaCode: Option[String],
                           country: Option[String]
                           )

  object ParsedAddress {

    def apply(fullAddress: String, knownAddressParts: Seq[AddressPart]): ParsedAddress = {

      def getAddressPart[T <: AddressPart: ClassTag](knownAddressParts: Seq[AddressPart]): Option[String] = {
        knownAddressParts.flatMap {
          case x: T => Some(cleanseStringSimple(x.value))
          case _ => None
        }.headOption
      }

      lazy val parsedFullAddress = parseAddress(fullAddress)
      lazy val combinedParsedAddress = knownAddressParts ++ parsedFullAddress

      // Get postcode and parse to get Sector, District and Area codes
      lazy val postcode = getAddressPart[Postcode](combinedParsedAddress)
      val parsedPostcode = parsePostcode(postcode)

      // Return prepopulated values for House number, Flat etc.. if supplied and apply some simple cleansing of the address
      this(
        fullAddress,
        cleanseStringSimple(fullAddress),
        getAddressPart[HouseNumber](combinedParsedAddress),
        getAddressPart[Flat](combinedParsedAddress),
        getAddressPart[Street](combinedParsedAddress),
        getAddressPart[Town](combinedParsedAddress),
        getAddressPart[City](combinedParsedAddress),
        postcode,
        getAddressPart[SectorCode](parsedPostcode),
        getAddressPart[DistrictCode](parsedPostcode),
        getAddressPart[AreaCode](parsedPostcode),
        getAddressPart[Country](combinedParsedAddress)
      )
    }
  }
}
