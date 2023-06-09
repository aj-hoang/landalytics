package com.landalytics.model.osopenuprn.clean

object CleanOsOpenUprnModel {

  case class OsOpenUprn(
                         uprn: String,
                         xCoordinate: Int,
                         yCoordinate: Int,
                         latitude: Double,
                         longitude: Double
                       )

}
