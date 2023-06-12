package com.landalytics.utilities.confighelpers

import io.circe._
import io.circe.generic.semiauto._
import io.circe.generic.auto._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class SampleJson(id: Int, value: String)
class TestClass extends ConfigExtractorTrait[SampleJson]

class ConfigExtractorTest extends AnyFlatSpec with Matchers {

  it should "load json file into case class" in {

    val jsonUri = getClass.getResource("/config.json").getPath

    val obj = new TestClass()

    val parsedJsonToCaseClass = obj.loadConfig(jsonUri)

    println(s"${parsedJsonToCaseClass.id} : ${parsedJsonToCaseClass.value}")

    // no runtime error
    succeed
  }

}
