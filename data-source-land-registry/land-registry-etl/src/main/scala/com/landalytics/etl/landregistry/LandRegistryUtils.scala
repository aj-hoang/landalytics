package com.landalytics.etl.landregistry

object LandRegistryUtils {

  def constructFullAddress(addressParts: Seq[Option[String]]): String = {
    addressParts.flatten.mkString(", ")
  }

}
