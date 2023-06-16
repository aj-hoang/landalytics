package com.landalytics.addressmatcher

object ConfigModel {

  case class AddressMatcherConfig(
                                 sourceUri: String,
                                 sourceIdField: String,
                                 addressCoreUri: String,
                                 addressMatchedUri: String,
                                 addressNotMatchedUri: String
                                 )

}
