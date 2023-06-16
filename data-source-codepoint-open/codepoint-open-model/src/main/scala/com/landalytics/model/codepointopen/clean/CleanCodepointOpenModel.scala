package com.landalytics.model.codepointopen.clean

object CleanCodepointOpenModel {

  case class CodepointOpen(
                           postcode: String,
                           positionalQualityIndicator: Int,
                           eastings: Int,
                           northings: Int,
                           countryCode: String,
                           nhsRegionalHACode: String,
                           nhsHACode: String,
                           adminCountyCode: String,
                           adminDistrictCode: String,
                           adminWardCode: String
                          )

}
