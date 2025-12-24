package com.thatdot.quine.v2api

import org.scalacheck.{Arbitrary, Gen}

import com.thatdot.api.v2.outputs.DestinationSteps.KafkaPropertyValue
import com.thatdot.api.v2.outputs.OutputFormat
import com.thatdot.quine.ScalaPrimitiveGenerators

object V2ApiCommonGenerators {

  import ScalaPrimitiveGenerators.Gens._

  object Gens {
    val outputFormat: Gen[OutputFormat] = Gen.oneOf(
      Gen.const(OutputFormat.JSON),
      for {
        schemaUrl <- nonEmptyAlphaNumStr.map(s => s"conf/schemas/$s.desc")
        typeName <- nonEmptyAlphaStr
      } yield OutputFormat.Protobuf(schemaUrl, typeName),
    )

    val kafkaPropertyValue: Gen[KafkaPropertyValue] =
      nonEmptyAlphaNumStr.map(KafkaPropertyValue.apply)

    val kafkaProperties: Gen[Map[String, KafkaPropertyValue]] =
      Gen.mapOf(Gen.zip(nonEmptyAlphaStr, kafkaPropertyValue))
  }

  object Arbs {
    implicit val outputFormat: Arbitrary[OutputFormat] = Arbitrary(Gens.outputFormat)
    implicit val kafkaPropertyValue: Arbitrary[KafkaPropertyValue] = Arbitrary(Gens.kafkaPropertyValue)
    implicit val kafkaProperties: Arbitrary[Map[String, KafkaPropertyValue]] = Arbitrary(Gens.kafkaProperties)
  }
}
