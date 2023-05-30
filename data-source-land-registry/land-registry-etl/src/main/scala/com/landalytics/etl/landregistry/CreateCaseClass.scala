package com.landalytics.etl.landregistry

import com.landalytics.model.landregistry.raw.RawLandRegistryModel.RawLandRegistry
import com.landalytics.model.landregistry.clean.CleanLandRegistryModel.LandRegistry
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Date

object CreateCaseClass {

  private def convertLrDateOfTransferStringToDate(dateString: String): Date = {
    Date.valueOf(dateString.substring(0,10))
  }

  def run(spark: SparkSession, sourceUri: String, destinationUri: String, saveMode: SaveMode = SaveMode.Overwrite) = {
    import spark.implicits._

    val rawLrDS = spark.read.parquet(sourceUri).as[RawLandRegistry]

    val lrDS = rawLrDS.map{ rawLr =>
      LandRegistry(
        transactionUniqueId = rawLr.transactionUniqueId,
        price = rawLr.price.map(_.toDouble),
        dateOfTransfer = rawLr.dateOfTransfer.map(convertLrDateOfTransferStringToDate),
        postcode = rawLr.postcode,
        propertyType = rawLr.propertyType,
        oldNew = rawLr.oldNew,
        duration = rawLr.duration,
        saon = rawLr.saon,
        paon = rawLr.paon,
        street = rawLr.street,
        locality = rawLr.locality,
        townCity = rawLr.townCity,
        district = rawLr.district,
        county = rawLr.county,
        ppdCategoryType = rawLr.ppdCategoryType,
        recordStatus = rawLr.recordStatus
      )
    }

    lrDS.write
      .mode(saveMode)
      .parquet(destinationUri)
  }

}
