package com.landalytics.etl.addresscore

import com.landalytics.model.epc.clean.CleanEPCModel.EPCSlim
import com.landalytics.model.osopenuprn.clean.CleanOsOpenUprnModel.OsOpenUprn
import com.landalytics.model.addresscore.AddressCoreModel._
import com.landalytics.model.addresscore.ConfigModel.AddressCoreConfig
import com.landalytics.utilities.etlhelpers.UUIDGenerator.generateUUID
import com.landalytics.utilities.sparkhelpers.ExtraConfigSparkRunner
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import io.circe.generic.auto._

object GenerateAddressCore extends ExtraConfigSparkRunner[AddressCoreConfig] {

  def aggregateEPC(epcDS: Dataset[EPCSlim]): Dataset[(String, EPCSlim)] = {
    import epcDS.sparkSession.implicits._
    epcDS.groupByKey(_.uprn).mapGroups { case (uprn, epc) => (uprn.getOrElse("Missing"), epc.next()) }
  }

  def joinOsOpenUprnWithEpc(osOpenUprnDS: Dataset[OsOpenUprn], epcDS: Dataset[(String, EPCSlim)]): Dataset[AddressCore] = {
    import osOpenUprnDS.sparkSession.implicits._

    osOpenUprnDS.joinWith(
      epcDS,
      osOpenUprnDS("uprn") === epcDS("_1"),
      "left"
    ).map{
      case(osOpenUprn, null) =>
        AddressCore(
          id = generateUUID(osOpenUprn.uprn).toString,
          geometry = AddressGeometry(osOpenUprn.latitude, osOpenUprn.longitude),
          geometrySourceId = osOpenUprn.uprn,
          geometrySourceType = "OS Open Uprn",
          fullAddress = None,
          parsedAddress = None,
          addressSourceId = None,
          addressSourceType = None
        )
      case (osOpenUprn, (_, epc)) =>
        AddressCore(
          id = generateUUID(osOpenUprn.uprn).toString,
          geometry = AddressGeometry(osOpenUprn.latitude, osOpenUprn.longitude),
          geometrySourceId = osOpenUprn.uprn,
          geometrySourceType = "OS Open Uprn",
          fullAddress = Some(epc.fullAddress),
          parsedAddress = Some(epc.parsedAddress),
          addressSourceId = Some(epc.lmkKey),
          addressSourceType = Some("EPC")
        )
    }
  }

  def run(spark: SparkSession, config: AddressCoreConfig): Unit = {

    // OS Open Uprns contain the UPRNS and coords
    // EPC contains addresses and the associated UPRN
    // By joining these 2 datasources, we can get the coords of the addresses
    import spark.implicits._


    val epcInputUri: String = config.generateAddressCoreConfig.epcInputUri
    val osOpenUprnInputUri: String = config.generateAddressCoreConfig.osOpenUprnInputUri
    val outputMatchedUri: String = config.generateAddressCoreConfig.addressCoreMatchedOutputUri
    val outputNotMatchedUri: String = config.generateAddressCoreConfig.addressCoreNotMatchedOutputUri


    val epcDS = spark.read.parquet(epcInputUri).as[EPCSlim]
    val osOpenUprnsDS = spark.read.parquet(osOpenUprnInputUri).as[OsOpenUprn]

    // EPC would have multiple EPCs for an address, need to aggregate, get the first address in the uprn aggregation
    val epcAggDS = aggregateEPC(epcDS)
    val joinedDS = joinOsOpenUprnWithEpc(osOpenUprnsDS, epcAggDS).cache

    // Write out entries which were matched
    joinedDS.filter(_.fullAddress.isDefined)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputMatchedUri)

    // write out entries which were not matched
    joinedDS.filter(_.fullAddress.isEmpty)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(outputNotMatchedUri)


  }

}
