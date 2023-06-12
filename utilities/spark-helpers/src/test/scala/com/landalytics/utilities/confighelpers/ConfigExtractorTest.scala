package com.landalytics.utilities.confighelpers

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import io.circe._
import io.circe.generic.semiauto._
// The below import is required for case class to inherit implicit decoders required to convert json into case classes
import io.circe.generic.auto._

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
