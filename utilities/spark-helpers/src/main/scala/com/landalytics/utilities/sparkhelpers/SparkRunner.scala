package com.landalytics.utilities.sparkhelpers

import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser

trait SparkRunner extends TypedSpark {

  def main(args: Array[String]) = {

    val parser = new OptionParser[SparkETLConfig]("landalytics-spark-runner") {
      head("landalytics-spark-runner", "1.0")
      opt[String]('s', "source-uri").required().valueName("<source-uri>").
        action((x, c) => c.copy(sourceUri = x)).
        text("Setting source uri is required")
      opt[String]('d', "destination-uri").required().valueName("<destination-uri>").
        action((x, c) => c.copy(destinationUri = x)).
        text("Setting destination uri is required")
    }

    parser.parse(args, SparkETLConfig()) match {
      case Some(config) =>
        // do stuff
        val spark = SparkSession.builder().master("local[*]").getOrCreate()

        // Extract values from config
        val configSourceUri = config.sourceUri
        val configDestinationUri = config.destinationUri

        // run
        run(spark, configSourceUri, configDestinationUri)
      case _ =>
        // Arguments are bad, errors will be displayed
    }


  }

}
