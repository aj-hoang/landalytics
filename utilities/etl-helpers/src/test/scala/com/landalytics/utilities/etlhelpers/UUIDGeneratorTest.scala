package com.landalytics.utilities.etlhelpers

import org.apache.commons.lang3.RandomUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UUIDGeneratorTest extends AnyFlatSpec with Matchers {
  def aString = new String(RandomUtils.nextBytes(1000))

  it should "generate a type 5 uuid from the a given input" in {
    UUIDGenerator.generateUUID(aString).toString.charAt(14) shouldEqual '5'
  }

  it should "generate the same guid for a given input" in {
    val input = aString
    val result =  UUIDGenerator.generateUUID(input)
    println(result.toString)
    UUIDGenerator.generateUUID(input) shouldEqual UUIDGenerator.generateUUID(input)
  }

}
