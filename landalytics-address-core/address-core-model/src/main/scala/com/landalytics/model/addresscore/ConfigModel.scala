package com.landalytics.model.addresscore

object ConfigModel {

  case class GenerateAddressCoreConfig(
                                      osOpenUprnInputUri: String,
                                      epcInputUri: String,
                                      addressCoreMatchedOutputUri: String,
                                      addressCoreNotMatchedOutputUri: String
                                      )
  case class AddressCoreConfig(
                              generateAddressCoreConfig: GenerateAddressCoreConfig
                              )
}
