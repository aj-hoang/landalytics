package com.landalytics.utilities.sparkhelpers

import com.landalytics.utilities.common.LandalyticsSparkScript
import io.circe.Decoder
import org.apache.spark.sql.SparkSession
import io.circe.generic.auto._
import io.circe.generic.semiauto._

trait ExtraConfigSpark[T]
  //extends ExtraConfigSparkClass[T]
    extends LandalyticsSparkScript {
  def run(spark: SparkSession, config: T): Unit

}
abstract class ExtraConfigSparkClass[T] {

//  def run(spark: SparkSession, config: T): Unit

}
