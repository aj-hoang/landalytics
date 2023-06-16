package com.landalytics.model.codepointopen.raw

object RawCodepointOpenModel {

  case class RawCodepointOpen(
                             postcode: String,
                             positionalQualityIndicator: String,
                             eastings: String,
                             northings: String,
                             countryCode: String,
                             nhsRegionalHACode: String,
                             nhsHACode: String,
                             adminCountyCode: String,
                             adminDistrictCode: String,
                             adminWardCode: String
                             )

}
