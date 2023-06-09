package com.landalytics.model.osopenuprn.raw

object RawOsOpenUprnModel {

  case class RawOsOpenUprn(
                          UPRN: String,
                          X_COORDINATE: String,
                          Y_COORDINATE: String,
                          LATITUDE: String,
                          LONGITUDE: String
                          )

}
