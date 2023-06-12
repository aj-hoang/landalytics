package com.landalytics.utilities.confighelpers

import io.circe.generic.semiauto._
import io.circe.generic.auto._
import io.circe.{Decoder, Encoder, Json}
import io.circe.parser.{decode, parse}
import io.circe.optics.JsonPath._

import java.io.File
import scala.io.Source

abstract class ConfigExtractor[T: Decoder] {

  def loadConfig(jsonUri: String): T = {
    def open(path: String) = new File(path)
    implicit class RichFile(file: File) {
      def read(): Iterator[String] = Source.fromFile(file).getLines()
    }

    // Get json from file, and try to parse
    val fileJson = open(jsonUri).read().mkString
    val parseResult = parse(fileJson)

    // Check parsed json result
    val json = parseResult match {
      case Left(parsingError) =>
        throw new IllegalArgumentException(s"Invalid JSON object: ${parsingError.message}")
      case Right(json) => // here we use the JSON object
        json
    }

    // Derive decoder to convert json into case class
//    lazy implicit val jsonDecoder: Decoder[T] = deriveDecoder[T]
    val decoded = decode[T](json.toString())

    decoded match {
      case Left(decodeError) =>
        throw decodeError
      case Right(decodedSuccess) =>
        decodedSuccess
    }

  }

}
