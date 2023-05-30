package com.landalytics.model.landregistry.clean

import java.sql.Date

object CleanLandRegistryModel {
  case class LandRegistry(
                           transcationUniqueId: String,
                           price: Option[Double],
                           dateOfTransfer: Option[Date],
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
