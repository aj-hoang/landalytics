package com.landalytics.utilities.sparkhelpers

sealed trait ExtraConfigSparkETLRootConfig {
  val configLocation: String
}
case class ExtraConfigSparkETLConfig (
                                       configLocation: String = ""
                                     ) extends ExtraConfigSparkETLRootConfig
