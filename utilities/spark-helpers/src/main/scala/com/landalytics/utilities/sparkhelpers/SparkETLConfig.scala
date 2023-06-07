package com.landalytics.utilities.sparkhelpers

sealed trait SparkETLRootConfig {
  val sourceUri: String
  val destinationUri: String
}
case class SparkETLConfig (
                           sourceUri: String = "",
                           destinationUri: String = ""
                           ) extends SparkETLRootConfig
