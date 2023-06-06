package com.landalytics.model.landregistry.clean

import com.landalytics.utilities.addressparsers.ParsedAddressModel.ParsedAddress

import java.sql.Date

object CleanLandRegistryModel {
//  case class LandRegistry(
//                           transactionUniqueId: String,
//                           fullAddress: String,
//                           price: Option[Double],
//                           dateOfTransfer: Option[Date],
//                           postcode: Option[String],
//                           propertyType: Option[String],
//                           oldNew: Option[String],
//                           duration: Option[String],
//                           paon: Option[String],
//                           saon: Option[String],
//                           street: Option[String],
//                           locality: Option[String],
//                           townCity: Option[String],
//                           district: Option[String],
//                           county: Option[String],
//                           ppdCategoryType: Option[String],
//                           recordStatus: Option[String]
//                            )

  case class LandRegistryTransaction(
                                     transactionUniqueId: String,
                                     price: Option[Double],
                                     dateOfTransfer: Option[Date],
                                     ppdCategoryType: Option[String],
                                     propertyType: Option[String],
                                     oldNew: Option[String],
                                     duration: Option[String]
                                     )


  case class LandRegistry(
                             transactionUniqueId: String,
                             landRegistryTransaction: LandRegistryTransaction,
                             fullAddress: String,
                             parsedAddress: ParsedAddress
                             )

  case class AggregatedLandRegistry(
                                    fullAddress: String,
                                    landRegistryTransactions: Seq[LandRegistryTransaction],
                                    parsedAddress: ParsedAddress
                                   )
}
