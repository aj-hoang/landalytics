package com.landalytics.etl.epc

import com.landalytics.model.epc.raw.RawEPCModel.RawEPC
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CreateCaseClassTest extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  it should "Create case class for EPC" in {

    val sampleRawDS = List(RawEPC(
       LMK_KEY = "Some-LMK-Key",
       ADDRESS1 = None,
       ADDRESS2 = None,
       ADDRESS3 = None,
       POSTCODE = Some("SW16 4QA"),
       BUILDING_REFERENCE_NUMBER = None,
       CURRENT_ENERGY_RATING = None,
       POTENTIAL_ENERGY_RATING = None,
       CURRENT_ENERGY_EFFICIENCY = None,
       POTENTIAL_ENERGY_EFFICIENCY = None,
       PROPERTY_TYPE = None,
       BUILT_FORM = None,
       INSPECTION_DATE = Some("2023-02-02"),
       LOCAL_AUTHORITY = None,
       CONSTITUENCY = None,
       COUNTY = None,
       LODGEMENT_DATE = None,
       TRANSACTION_TYPE = None,
       ENVIRONMENT_IMPACT_CURRENT = None,
       ENVIRONMENT_IMPACT_POTENTIAL = None,
       ENERGY_CONSUMPTION_CURRENT = None,
       ENERGY_CONSUMPTION_POTENTIAL = None,
       CO2_EMISSIONS_CURRENT = None,
       CO2_EMISS_CURR_PER_FLOOR_AREA = None,
       CO2_EMISSIONS_POTENTIAL = None,
       LIGHTING_COST_CURRENT = None,
       LIGHTING_COST_POTENTIAL = None,
       HEATING_COST_CURRENT = None,
       HEATING_COST_POTENTIAL = None,
       HOT_WATER_COST_CURRENT = None,
       HOT_WATER_COST_POTENTIAL = None,
       TOTAL_FLOOR_AREA = None,
       ENERGY_TARIFF = None,
       MAINS_GAS_FLAG = None,
       FLOOR_LEVEL = None,
       FLAT_TOP_STOREY = None,
       FLAT_STOREY_COUNT = None,
       MAIN_HEATING_CONTROLS = None,
       MULTI_GLAZE_PROPORTION = None,
       GLAZED_TYPE = None,
       GLAZED_AREA = None,
       EXTENSION_COUNT = None,
       NUMBER_HABITABLE_ROOMS = None,
       NUMBER_HEATED_ROOMS = None,
       LOW_ENERGY_LIGHTING = None,
       NUMBER_OPEN_FIREPLACES = None,
       HOTWATER_DESCRIPTION = None,
       HOT_WATER_ENERGY_EFF = None,
       HOT_WATER_ENV_EFF = None,
       FLOOR_DESCRIPTION = None,
       FLOOR_ENERGY_EFF = None,
       FLOOR_ENV_EFF = None,
       WINDOWS_DESCRIPTION = None,
       WINDOWS_ENERGY_EFF = None,
       WINDOWS_ENV_EFF = None,
       WALLS_DESCRIPTION = None,
       WALLS_ENERGY_EFF = None,
       WALLS_ENV_EFF = None,
       SECONDHEAT_DESCRIPTION = None,
       SHEATING_ENERGY_EFF = None,
       SHEATING_ENV_EFF = None,
       ROOF_DESCRIPTION = None,
       ROOF_ENERGY_EFF = None,
       ROOF_ENV_EFF = None,
       MAINHEAT_DESCRIPTION = None,
       MAINHEAT_ENERGY_EFF = None,
       MAINHEAT_ENV_EFF = None,
       MAINHEATCONT_DESCRIPTION = None,
       MAINHEATC_ENERGY_EFF = None,
       MAINHEATC_ENV_EFF = None,
       LIGHTING_DESCRIPTION = None,
       LIGHTING_ENERGY_EFF = None,
       LIGHTING_ENV_EFF = None,
       MAIN_FUEL = None,
       WIND_TURBINE_COUNT = None,
       HEAT_LOSS_CORRIDOR = None,
       UNHEATED_CORRIDOR_LENGTH = None,
       FLOOR_HEIGHT = None,
       PHOTO_SUPPLY = None,
       SOLAR_WATER_HEATING_FLAG = None,
       MECHANICAL_VENTILATION = None,
       ADDRESS = Some("36, Stanford Road"),
       LOCAL_AUTHORITY_LABEL = None,
       CONSTITUENCY_LABEL = None,
       POSTTOWN = Some("LONDON"),
       CONSTRUCTION_AGE_BAND = None,
       LODGEMENT_DATETIME = None,
       TENURE = None,
       FIXED_LIGHTING_OUTLETS_COUNT = None,
       LOW_ENERGY_FIXED_LIGHT_COUNT = None,
       UPRN = Some("Some UPRN"),
       UPRN_SOURCE = Some("Addressbase")
    )).toDS
    
    val baseUri = getClass.getResource("/CreateCaseClass/epc").getPath
    val sampleRawEpcUri = baseUri ++ "/raw/epc.parquet"
    sampleRawDS.write
      .mode(SaveMode.Overwrite)
      .parquet(sampleRawEpcUri)

    // Use above created parquet in createCaseClass call
    val sourceUri = sampleRawEpcUri
    val destinationUri = baseUri ++ "/cleanModel/epc.parquet"
    println(sourceUri)
    println(destinationUri)

    CreateCaseClass.run(spark, sourceUri, destinationUri)

    spark.read.parquet(destinationUri).printSchema()
    spark.read.parquet(destinationUri).show(false)

    // should have no runtime errors
    succeed


  }

}
