package com.landalytics.model.osopenuprn

object ConfigModel {

  case class OsOpenUprnConfig(
                               csvUri: String,
                               rawParquetUri: String,
                               createCaseClassParquetUri: String
                             )

}
