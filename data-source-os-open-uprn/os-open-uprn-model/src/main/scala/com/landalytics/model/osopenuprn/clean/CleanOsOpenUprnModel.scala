package com.landalytics.model.osopenuprn.clean

object CleanOsOpenUprnModel {

  case class OsOpenUprn(
                         uprn: String,
                         xCoordinate: Double,
                         yCoordinate: Double,
                         latitude: Double,
                         longitude: Double
                       )

}
