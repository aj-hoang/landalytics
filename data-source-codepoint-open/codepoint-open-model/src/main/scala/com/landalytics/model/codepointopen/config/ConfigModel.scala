package com.landalytics.model.codepointopen.config

object ConfigModel {

  case class CodepointOpenConfig(
                                csvUri: String,
                                rawParquetUri: String,
                                caseClassParquetUri: String
                                )

}
