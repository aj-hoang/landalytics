package com.landalytics.etl.epc

import com.landalytics.model.epc.raw.RawEPCModel.RawEPC
import com.landalytics.model.epc.clean.CleanEPCModel._
import com.landalytics.utilities.addressparsers.ParsedAddressModel.{AddressPart, Country, ParsedAddress, Postcode}
import com.landalytics.utilities.sparkhelpers.SparkRunner
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.Date

object CreateCaseClass extends SparkRunner {

  def run(spark: SparkSession, sourceUri: String, destinationUri: String): Unit = {
    import spark.implicits._

    def convertDateStringToDate(s: String): Date = Date.valueOf(s)

    val rawEpcDS = spark.read.parquet(sourceUri).as[RawEPC]

    val epcDS = rawEpcDS.map{ rawEpc =>
      val fullAddress = Seq(rawEpc.ADDRESS, rawEpc.POSTTOWN, rawEpc.POSTCODE).flatten.mkString(", ")
      val knownAddressParts: Seq[AddressPart] = Seq(
        rawEpc.POSTCODE.map(Postcode),
        Some(Country("UK"))
      ).flatten

      EPC(
        lmkKey = rawEpc.LMK_KEY,
        fullAddress = fullAddress,
        parsedAddress = ParsedAddress(fullAddress, knownAddressParts),
        epcDetails = EPCDetails(
          lmkKey = rawEpc.LMK_KEY,
          buildingReferenceNumber = rawEpc.BUILDING_REFERENCE_NUMBER,
          currentEnergyRating = rawEpc.CURRENT_ENERGY_RATING,
          potentialEnergyRating = rawEpc.POTENTIAL_ENERGY_RATING,
          currentEnergyEfficiency = rawEpc.CURRENT_ENERGY_EFFICIENCY,
          potentialEnergyEfficiency = rawEpc.POTENTIAL_ENERGY_EFFICIENCY,
          propertyType = rawEpc.PROPERTY_TYPE,
          builtForm = rawEpc.BUILT_FORM,
          inspectionDate = rawEpc.INSPECTION_DATE.map(convertDateStringToDate),
          localAuthority = rawEpc.LOCAL_AUTHORITY,
          constituency = rawEpc.CONSTITUENCY,
          county = rawEpc.COUNTY,
          lodgementDate = rawEpc.LODGEMENT_DATE.map(convertDateStringToDate),
          transactionType = rawEpc.TRANSACTION_TYPE,
          environmentImpactCurrent = rawEpc.ENVIRONMENT_IMPACT_CURRENT,
          environmentImpactPotential = rawEpc.ENVIRONMENT_IMPACT_POTENTIAL,
          energyConsumptionCurrent = rawEpc.ENERGY_CONSUMPTION_CURRENT,
          energyConsumptionPotential = rawEpc.ENERGY_CONSUMPTION_POTENTIAL,
          co2EmissionsCurrent = rawEpc.CO2_EMISSIONS_CURRENT,
          co2EmissCurrPerFloorArea = rawEpc.CO2_EMISS_CURR_PER_FLOOR_AREA,
          co2EmissionsPotential = rawEpc.CO2_EMISSIONS_POTENTIAL,
          lightingCostCurrent = rawEpc.LIGHTING_COST_CURRENT,
          lightingCostPotential = rawEpc.LIGHTING_COST_POTENTIAL,
          heatingCostCurrent = rawEpc.HEATING_COST_CURRENT,
          heatingCostPotential = rawEpc.HEATING_COST_POTENTIAL,
          hotWaterCostCurrent = rawEpc.HOT_WATER_COST_CURRENT,
          hotWaterCostPotential = rawEpc.HOT_WATER_COST_POTENTIAL,
          totalFloorArea = rawEpc.TOTAL_FLOOR_AREA.map(_.toDouble),
          energyTariff = rawEpc.ENERGY_TARIFF,
          mainsGasFlag = rawEpc.MAINS_GAS_FLAG,
          floorLevel = rawEpc.FLOOR_LEVEL,
          flatTopStorey = rawEpc.FLAT_TOP_STOREY,
          flatStoreyCount = rawEpc.FLAT_STOREY_COUNT,
          mainHeatingControls = rawEpc.MAIN_HEATING_CONTROLS,
          multiGlazeProportion = rawEpc.MULTI_GLAZE_PROPORTION,
          glazedType = rawEpc.GLAZED_TYPE,
          glazedArea = rawEpc.GLAZED_AREA,
          extensionCount = rawEpc.EXTENSION_COUNT,
          numberHabitableRooms = rawEpc.NUMBER_HABITABLE_ROOMS,
          numberHeatedRooms = rawEpc.NUMBER_HEATED_ROOMS,
          lowEnergyLighting = rawEpc.LOW_ENERGY_LIGHTING,
          numberOpenFireplaces = rawEpc.NUMBER_OPEN_FIREPLACES,
          hotwaterDescription = rawEpc.HOTWATER_DESCRIPTION,
          hotWaterEnergyEff = rawEpc.HOT_WATER_ENERGY_EFF,
          hotWaterEnvEff = rawEpc.HOT_WATER_ENV_EFF,
          floorDescription = rawEpc.FLOOR_DESCRIPTION,
          floorEnergyEff = rawEpc.FLOOR_ENERGY_EFF,
          floorEnvEff = rawEpc.FLOOR_ENV_EFF,
          windowsDescription = rawEpc.WINDOWS_DESCRIPTION,
          windowsEnergyEff = rawEpc.WINDOWS_ENERGY_EFF,
          windowsEnvEff = rawEpc.WINDOWS_ENV_EFF,
          wallsDescription = rawEpc.WALLS_DESCRIPTION,
          wallsEnergyEff = rawEpc.WALLS_ENERGY_EFF,
          wallsEnvEff = rawEpc.WALLS_ENV_EFF,
          secondheatDescription = rawEpc.SECONDHEAT_DESCRIPTION,
          sheatingEnergyEff = rawEpc.SHEATING_ENERGY_EFF,
          sheatingEnvEff = rawEpc.SHEATING_ENV_EFF,
          roofDescription = rawEpc.ROOF_DESCRIPTION,
          roofEnergyEff = rawEpc.ROOF_ENERGY_EFF,
          roofEnvEff = rawEpc.ROOF_ENV_EFF,
          mainheatDescription = rawEpc.MAINHEAT_DESCRIPTION,
          mainheatEnergyEff = rawEpc.MAINHEAT_ENERGY_EFF,
          mainheatEnvEff = rawEpc.MAINHEAT_ENV_EFF,
          mainheatcontDescription = rawEpc.MAINHEATCONT_DESCRIPTION,
          mainheatcEnergyEff = rawEpc.MAINHEATC_ENERGY_EFF,
          mainheatcEnvEff = rawEpc.MAINHEATC_ENV_EFF,
          lightingDescription = rawEpc.LIGHTING_DESCRIPTION,
          lightingEnergyEff = rawEpc.LIGHTING_ENERGY_EFF,
          lightingEnvEff = rawEpc.LIGHTING_ENV_EFF,
          mainFuel = rawEpc.MAIN_FUEL,
          windTurbineCount = rawEpc.WIND_TURBINE_COUNT,
          heatLossCorridor = rawEpc.HEAT_LOSS_CORRIDOR,
          unheatedCorridorLength = rawEpc.UNHEATED_CORRIDOR_LENGTH,
          floorHeight = rawEpc.FLOOR_HEIGHT,
          photoSupply = rawEpc.PHOTO_SUPPLY,
          solarWaterHeatingFlag = rawEpc.SOLAR_WATER_HEATING_FLAG,
          mechanicalVentilation = rawEpc.MECHANICAL_VENTILATION,
          address = rawEpc.ADDRESS,
          localAuthorityLabel = rawEpc.LOCAL_AUTHORITY_LABEL,
          constituencyLabel = rawEpc.CONSTITUENCY_LABEL,
          posttown = rawEpc.POSTTOWN,
          constructionAgeBand = rawEpc.CONSTRUCTION_AGE_BAND,
          lodgementDatetime = rawEpc.LODGEMENT_DATETIME,
          tenure = rawEpc.TENURE,
          fixedLightingOutletsCount = rawEpc.FIXED_LIGHTING_OUTLETS_COUNT,
          lowEnergyFixedLightCount = rawEpc.LOW_ENERGY_FIXED_LIGHT_COUNT
        ),
        uprn = rawEpc.UPRN,
        uprnSource = rawEpc.UPRN_SOURCE
      )

    }

    epcDS.write
      .mode(SaveMode.Overwrite)
      .parquet(destinationUri)

  }

}
