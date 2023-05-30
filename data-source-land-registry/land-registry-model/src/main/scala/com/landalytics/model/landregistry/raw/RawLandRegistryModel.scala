package com.landalytics.model.landregistry.raw

object RawLandRegistryModel {

  case class RawLandRegistry(
                            transactionUniqueId: String,
                            price: Option[String],
                            dateOfTransfer: Option[String],
                            postcode: Option[String],
                            propertyType: Option[String],
                            oldNew: Option[String],
                            duration: Option[String],
                            paon: Option[String],
                            saon: Option[String],
                            street: Option[String],
                            locality: Option[String],
                            townCity: Option[String],
                            district: Option[String],
                            county: Option[String],
                            ppdCategoryType: Option[String],
                            recordStatus: Option[String]
                            )

}
