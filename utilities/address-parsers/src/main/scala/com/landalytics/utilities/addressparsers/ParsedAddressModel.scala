package com.landalytics.utilities.addressparsers

import scala.reflect.ClassTag
import com.mapzen.jpostal.AddressParser

object ParsedAddressModel {

  def parseAddress(address: String) = {
    val addressParser: AddressParser = AddressParser.getInstance()

    val parsedAddress = addressParser.parseAddress(address)

    parsedAddress.foreach( c =>
      println(s"label: ${c.getLabel} value: ${c.getValue}")
    )

  }
  sealed trait AddressPart {
    val value: String
  }
  case class Paon(value: String) extends AddressPart
  case class Saon(value: String) extends AddressPart
  case class Street(value: String) extends AddressPart
  case class Locality(value: String) extends AddressPart
  case class TownCity(value: String) extends AddressPart
  case class Country(value: String) extends AddressPart

  case class ParsedAddress(
                           fullAddress: String,
                           paon: Option[String],
                           saon: Option[String],
                           street: Option[String],
                           locality: Option[String],
                           townCity: Option[String],
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

      // Return prepopulated values for Paon, Saon etc.. if supplied
      this(
        fullAddress,
        getAddressPart[Paon](knownAddressParts),
        getAddressPart[Saon](knownAddressParts),
        getAddressPart[Street](knownAddressParts),
        getAddressPart[Locality](knownAddressParts),
        getAddressPart[TownCity](knownAddressParts),
        getAddressPart[Country](knownAddressParts)
      )
    }
  }
}
