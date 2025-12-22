package com.thatdot.quine.v2api

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.api.v2.outputs.DestinationSteps.KafkaPropertyValue
import com.thatdot.api.v2.outputs.OutputFormat
import com.thatdot.quine.{ArbitraryAwsTypes, ScalaPrimitiveGenerators}

trait ArbitraryV2ApiCommon extends ArbitraryAwsTypes {
  import ScalaPrimitiveGenerators.Gens.nonEmptyAlphaStr

  implicit val genOutputFormat: Gen[OutputFormat] = Gen.oneOf(
    Gen.const(OutputFormat.JSON),
    for {
      schemaUrl <- Gen.alphaNumStr.map(s => s"conf/schemas/$s.desc")
      typeName <- Gen.alphaStr.suchThat(_.nonEmpty)
    } yield OutputFormat.Protobuf(schemaUrl, typeName),
  )

  implicit val arbOutputFormat: Arbitrary[OutputFormat] = Arbitrary(genOutputFormat)

  implicit val genKafkaPropertyValue: Gen[KafkaPropertyValue] =
    Gen.alphaNumStr.map(KafkaPropertyValue.apply)

  implicit val arbKafkaProperties: Arbitrary[Map[String, KafkaPropertyValue]] = Arbitrary(
    Gen.mapOf(
      Gen.zip(nonEmptyAlphaStr, genKafkaPropertyValue),
    ),
  )
}
