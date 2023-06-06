package com.landalytics.etl.landregistry

import com.landalytics.etl.landregistry.LandRegistryUtils.constructFullAddress
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LandRegistryUtilsTest extends AnyFlatSpec with Matchers {

  it should "combine address parts" in {

    val input = Seq(Some("20"), None, Some("foobar street"), Some("London"))
    val actual = constructFullAddress(input)

    actual shouldEqual "20, foobar street, London"
  }

}
