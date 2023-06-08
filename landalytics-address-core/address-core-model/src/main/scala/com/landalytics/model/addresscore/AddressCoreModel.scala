package com.landalytics.model.addresscore

import com.landalytics.utilities.addressparsers.ParsedAddressModel.ParsedAddress

object AddressCoreModel {

  case class AddressCore (
                         id: String, // Unique reproducible id, could be hash of geometrySourceId and coords
                         geometry: AddressGeometry,
                         geometrySourceId: String,
                         geometrySourceType: String,
                         fullAddress: Option[String],
                         parsedAddress: Option[ParsedAddress],
                         addressSourceId: Option[String],
                         addressSourceType: Option[String]
                         )

  case class AddressGeometry(
                            latitude: Double,
                            longitude: Double
                            )

}
