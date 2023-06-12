package com.landalytics.utilities.confighelpers

object ConfigModels {

  case class OsOpenUprnConfig(
                             importRawToParquetSourceUri: String,
                             importRawToParquetDestinationUri: String,
                             createCaseClassSourceUri: String,
                             createCaseClassDestinationUri: String
                             )


}
