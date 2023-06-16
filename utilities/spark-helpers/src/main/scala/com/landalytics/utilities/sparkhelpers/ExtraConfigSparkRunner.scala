package com.landalytics.utilities.sparkhelpers

import com.landalytics.utilities.confighelpers.{ConfigExtractor, ConfigExtractorTrait}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser
import io.circe.generic.auto._
import io.circe.generic.semiauto._
import io.circe._
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.serializer.KryoSerializer

abstract class ExtraConfigSparkRunner[T: Decoder] extends ConfigExtractor[T] with ExtraConfigSpark[T] {
  def main(args: Array[String]) = {

    val parser = new OptionParser[ExtraConfigSparkETLConfig]("landalytics-extra-config-spark-runner") {
      head("landalytics-extra-config-spark-runner", "1.0")
      opt[String]('c', "config-file").required().valueName("<config-file>").
        action((x, c) => c.copy(configLocation = x)).
        text("Setting config-file is required")
    }

    parser.parse(args, ExtraConfigSparkETLConfig()) match {
      case Some(config) =>
        // load config into case class
        val jsonConfig: T = loadConfig(config.configLocation)

        val spark = if (useSedona) {
          val sedonaSpark = SparkSession.builder().master("local[*]")
            .config("spark.serializer", classOf[KryoSerializer].getName)
            .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
            .getOrCreate()

          SedonaSQLRegistrator.registerAll(sedonaSpark)
          sedonaSpark
        } else {
          SparkSession.builder().master("local[*]").getOrCreate()
        }

//        val spark = SparkSession.builder().master("local[*]").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        // run with config values
        run(spark, jsonConfig)
      case _ =>
      // Arguments are bad, errors will be displayed
    }


  }
}
